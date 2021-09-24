package distpow

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

/****** Tracer structs ******/
type WorkerConfig struct {
	WorkerID         string
	ListenAddr       string
	CoordAddr        string
	TracerServerAddr string
	TracerSecret     []byte
}

type WorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type WorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
}

type WorkerResultWithToken struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	Token 			 tracing.TracingToken
}

type WorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

/****** RPC structs ******/
type WorkerMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	WorkerBits       uint
	Token			 tracing.TracingToken
}

type WorkerCancelArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type WorkerFoundArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret 		     []uint8
	Token			 tracing.TracingToken
}

type WorkerFoundReply struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret 		     []uint8
	Token			 tracing.TracingToken
}

type CancelChan chan struct{}

type Worker struct {
	config        WorkerConfig
	Tracer        *tracing.Tracer
	Coordinator   *rpc.Client
	mineTasks     map[string]CancelChan
	ResultChannel chan WorkerResultWithToken
}

type WorkerMineTasks struct {
	mu    sync.Mutex
	tasks map[string]CancelChan
}

type WorkerResultCache struct {
	mu    sync.Mutex
	cache map[string]WorkerCacheEntry
}

type WorkerCacheEntry struct {
	NumTrailingZeros uint
	Secret           []uint8
}

type WorkerRPCHandler struct {
	tracer      *tracing.Tracer
	coordinator *rpc.Client
	mineTasks   WorkerMineTasks
	resultChan  chan WorkerResultWithToken
	resultCache WorkerResultCache
}

func NewWorker(config WorkerConfig) *Worker {
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: config.WorkerID,
		Secret:         config.TracerSecret,
	})

	coordClient, err := rpc.Dial("tcp", config.CoordAddr)
	if err != nil {
		log.Fatal("failed to dail Coordinator:", err)
	}

	return &Worker{
		config:        config,
		Tracer:        tracer,
		Coordinator:   coordClient,
		mineTasks:     make(map[string]CancelChan),
		ResultChannel: make(chan WorkerResultWithToken),
	}
}

func (w *Worker) InitializeWorkerRPCs() error {
	server := rpc.NewServer()
	err := server.Register(&WorkerRPCHandler{
		tracer:      w.Tracer,
		coordinator: w.Coordinator,
		mineTasks: WorkerMineTasks{
			tasks: make(map[string]CancelChan),
		},
		resultChan: w.ResultChannel,
		resultCache: WorkerResultCache{
			cache: make(map[string]WorkerCacheEntry),
		},
	})

	// publish Worker RPCs
	if err != nil {
		return fmt.Errorf("format of Worker RPCs aren't correct: %s", err)
	}

	listener, e := net.Listen("tcp", w.config.ListenAddr)
	if e != nil {
		return fmt.Errorf("%s listen error: %s", w.config.WorkerID, e)
	}

	log.Printf("Serving %s RPCs on port %s", w.config.WorkerID, w.config.ListenAddr)
	go server.Accept(listener)

	return nil
}

// Mine is a non-blocking async RPC from the Coordinator
// instructing the worker to solve a specific pow instance.
func (w *WorkerRPCHandler) Mine(args WorkerMineArgs, reply *struct{}) error {

	// add new task
	cancelCh := make(chan struct{}, 1)
	w.mineTasks.set(args.Nonce, args.NumTrailingZeros, args.WorkerByte, cancelCh)

	trace := w.tracer.ReceiveToken(args.Token)

	trace.RecordAction(WorkerMine{
		Nonce:            args.Nonce,
		NumTrailingZeros: args.NumTrailingZeros,
		WorkerByte:       args.WorkerByte,
	})
	go miner(w, args, cancelCh, trace)

	return nil
}

// Cancel is a non-blocking async RPC from the Coordinator
// instructing the worker to stop solving a specific pow instance.
func (w *WorkerRPCHandler) Cancel(args WorkerCancelArgs, reply *struct{}) error {
	cancelChan, ok := w.mineTasks.get(args.Nonce, args.NumTrailingZeros, args.WorkerByte)
	if !ok {
		log.Fatalf("Received more than once cancellation for %s", generateWorkerTaskKey(args.Nonce, args.NumTrailingZeros, args.WorkerByte))
	}
	cancelChan <- struct{}{}
	// delete the task here, and the worker should terminate + send something back very soon
	w.mineTasks.delete(args.Nonce, args.NumTrailingZeros, args.WorkerByte)
	return nil
}

// Cancel is a non-blocking async RPC from the Coordinator
// instructing the worker to stop solving a specific pow instance.
func (w *WorkerRPCHandler) Found(args WorkerFoundArgs, reply *struct{}) error {
	cancelChan, ok := w.mineTasks.get(args.Nonce, args.NumTrailingZeros, args.WorkerByte)
	trace := w.tracer.ReceiveToken(args.Token)
	if ok {
		// try to add result to cache
		w.resultCache.cacheAdd(args.Nonce, args.NumTrailingZeros, args.Secret, trace)
		// First "FOUND" RPC of cancellations
		cancelChan <- struct{}{}
		// delete the task here, and the worker should terminate + send something back very soon
		w.mineTasks.delete(args.Nonce, args.NumTrailingZeros, args.WorkerByte)
	} else {
		// Record Cancel
		// This is when no active mining is happening, just need to update cache.
		trace.RecordAction(WorkerCancel{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
		})
		// try to add result to cache
		w.resultCache.cacheAdd(args.Nonce, args.NumTrailingZeros, args.Secret, trace)
		// confirm back to coordinator
		w.resultChan <- WorkerResultWithToken{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           nil,
			Token:			  trace.GenerateToken(),
		}
	}
	return nil
}

func nextChunk(chunk []uint8) []uint8 {
	for i := 0; i < len(chunk); i++ {
		if chunk[i] == 0xFF {
			chunk[i] = 0
		} else {
			chunk[i]++
			return chunk
		}
	}
	return append(chunk, 1)
}

func hasNumZeroesSuffix(str []byte, numZeroes uint) bool {
	var trailingZeroesFound uint
	for i := len(str) - 1; i >= 0; i-- {
		if str[i] == '0' {
			trailingZeroesFound++
		} else {
			break
		}
	}
	return trailingZeroesFound >= numZeroes
}

func miner(w *WorkerRPCHandler, args WorkerMineArgs, killChan <-chan struct{}, trace *tracing.Trace) {

	// Check cache first, mine otherwise
	cacheResult := w.resultCache.cacheGet(args.Nonce, args.NumTrailingZeros, trace)
	if cacheResult != nil {
		result := WorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           cacheResult,
		}
		trace.RecordAction(result)

		// Send Result with token
		resultWithToken := WorkerResultWithToken{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           cacheResult,
			Token:            trace.GenerateToken(),
		}
		w.resultChan <- resultWithToken

		<-killChan

		// ACK the cancellation; the coordinator will be waiting for this.
		// and log it, which satisfies the (optional) stricter interpretation of WorkerCancel
		trace.RecordAction(WorkerCancel{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
		})

		w.resultChan <- WorkerResultWithToken{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           nil,
			Token:			  trace.GenerateToken(),
		}
		return
	}

	chunk := []uint8{}
	remainderBits := 8 - (args.WorkerBits % 9)

	hashStrBuf, wholeBuffer := new(bytes.Buffer), new(bytes.Buffer)
	if _, err := wholeBuffer.Write(args.Nonce); err != nil {
		panic(err)
	}
	wholeBufferTrunc := wholeBuffer.Len()

	// table out all possible "thread bytes", aka the byte prefix
	// between the nonce and the bytes explored by this worker
	remainderEnd := 1 << remainderBits
	threadBytes := make([]uint8, remainderEnd)
	for i := 0; i < remainderEnd; i++ {
		threadBytes[i] = uint8((int(args.WorkerByte) << remainderBits) | i)
	}

	for {
		for _, threadByte := range threadBytes {
			select {
			case <-killChan:
				trace.RecordAction(WorkerCancel{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
				})
				w.resultChan <- WorkerResultWithToken{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil, // nil secret treated as cancel completion
					Token:			  trace.GenerateToken(),
				}
				// send an extra to satisfy first round of cancellations
				w.resultChan <- WorkerResultWithToken{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil, // nil secret treated as cancel completion
					Token:			  trace.GenerateToken(),
				}
				return
			default:
				// pass
			}
			wholeBuffer.Truncate(wholeBufferTrunc)
			if err := wholeBuffer.WriteByte(threadByte); err != nil {
				panic(err)
			}
			if _, err := wholeBuffer.Write(chunk); err != nil {
				panic(err)
			}
			hash := md5.Sum(wholeBuffer.Bytes())
			hashStrBuf.Reset()
			fmt.Fprintf(hashStrBuf, "%x", hash)
			if hasNumZeroesSuffix(hashStrBuf.Bytes(), args.NumTrailingZeros) {
				result := WorkerResult{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           wholeBuffer.Bytes()[wholeBufferTrunc:],
				}
				trace.RecordAction(result)

				// Send Result with token
				resultWithToken := WorkerResultWithToken{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           wholeBuffer.Bytes()[wholeBufferTrunc:],
					Token:            trace.GenerateToken(),
				}
				w.resultChan <- resultWithToken

				// now, wait for the worker the receive a cancellation,
				// which the coordinator should always send no matter what.
				// note: this position takes care of interleavings where cancellation comes after we check killChan but
				//       before we log the result we found, forcing WorkerCancel to be the last action logged, even in that case.
				<-killChan

				// ACK the cancellation; the coordinator will be waiting for this.
				// and log it, which satisfies the (optional) stricter interpretation of WorkerCancel
				trace.RecordAction(WorkerCancel{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
				})

				w.resultChan <- WorkerResultWithToken{
					Nonce:            args.Nonce,
					NumTrailingZeros: args.NumTrailingZeros,
					WorkerByte:       args.WorkerByte,
					Secret:           nil,
					Token:			  trace.GenerateToken(),
				}
				return
			}
		}
		chunk = nextChunk(chunk)
	}
}

func (t *WorkerMineTasks) get(nonce []uint8, numTrailingZeros uint, workerByte uint8) (CancelChan, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	_, ok := t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)]
	return t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)], ok
}

func (t *WorkerMineTasks) set(nonce []uint8, numTrailingZeros uint, workerByte uint8, val CancelChan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[generateWorkerTaskKey(nonce, numTrailingZeros, workerByte)] = val
}

func (t *WorkerMineTasks) delete(nonce []uint8, numTrailingZeros uint, workerByte uint8) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, generateWorkerTaskKey(nonce, numTrailingZeros, workerByte))
}

//**Functions for accessing cache**//
func (t *WorkerResultCache) cacheGet(nonce []uint8, numTrailingZeros uint, trace *tracing.Trace) []uint8 {
	t.mu.Lock()
	defer t.mu.Unlock()
	cacheEntry, found := t.cache[generateWorkerCacheKey(nonce)]

	if found == false {
		// No entry for nonce
		trace.RecordAction(CacheMiss{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeros,
		})
		return nil
	} else if cacheEntry.NumTrailingZeros >= numTrailingZeros {
		// Entry for nonce with equal or greater number of zeroes
		trace.RecordAction(CacheHit{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeros,
			Secret:           cacheEntry.Secret,
		})
		return cacheEntry.Secret
	} else {
		// default
		trace.RecordAction(CacheMiss{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeros,
		})
		return nil
	}
}

func (t *WorkerResultCache) cacheAdd(nonce []uint8, numTrailingZeros uint, secret []uint8, trace *tracing.Trace) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cacheEntry, found := t.cache[generateWorkerCacheKey(nonce)]
	if found == false {
		// No entry exists
		t.cache[generateWorkerCacheKey(nonce)] = WorkerCacheEntry{
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		}
		trace.RecordAction(CacheAdd{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		})
	} else if numTrailingZeros > cacheEntry.NumTrailingZeros {
		// Dominates by number of trailing zeroes, must replace
		trace.RecordAction(CacheRemove{
			Nonce:            nonce,
			NumTrailingZeros: cacheEntry.NumTrailingZeros,
			Secret:           cacheEntry.Secret,
		})
		delete(t.cache, generateWorkerCacheKey(nonce))

		trace.RecordAction(CacheAdd{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		})
		t.cache[generateWorkerCacheKey(nonce)] = WorkerCacheEntry{
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		}
	} else if numTrailingZeros == cacheEntry.NumTrailingZeros && bytes.Compare(secret, cacheEntry.Secret) > 0 {
		// Dominates by secret, must replace
		trace.RecordAction(CacheRemove{
			Nonce:            nonce,
			NumTrailingZeros: cacheEntry.NumTrailingZeros,
			Secret:           cacheEntry.Secret,
		})
		delete(t.cache, generateWorkerCacheKey(nonce))

		trace.RecordAction(CacheAdd{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		})
		t.cache[generateWorkerCacheKey(nonce)] = WorkerCacheEntry{
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		}
	}
}

func generateWorkerTaskKey(nonce []uint8, numTrailingZeros uint, workerByte uint8) string {
	return fmt.Sprintf("%s|%d|%d", hex.EncodeToString(nonce), numTrailingZeros, workerByte)
}

func generateWorkerCacheKey(nonce []uint8) string {
	return string(nonce)
}
