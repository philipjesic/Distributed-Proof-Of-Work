package distpow

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type WorkerAddr string

type WorkerClient struct {
	addr       WorkerAddr
	client     *rpc.Client
	workerByte uint8
}

type CoordinatorConfig struct {
	ClientAPIListenAddr string
	WorkerAPIListenAddr string
	Workers             []WorkerAddr
	TracerServerAddr    string
	TracerSecret        []byte
}

type CoordinatorMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CoordinatorWorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorWorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
}

type CoordinatorWorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorSuccess struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type Coordinator struct {
	config  CoordinatorConfig
	tracer  *tracing.Tracer
	workers []*WorkerClient
}

/****** RPC structs ******/
type CoordMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Token            tracing.TracingToken
}

type CoordMineResponse struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
	Token 			 tracing.TracingToken
}

type CoordResultArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	Token            tracing.TracingToken
}

type ResultChan chan CoordResultArgs

type CoordRPCHandler struct {
	tracer     *tracing.Tracer
	workers    []*WorkerClient
	workerBits uint
	mineTasks  CoordinatorMineTasks
	resultCache CoordinatorResultCache
}

type CoordinatorMineTasks struct {
	mu    sync.Mutex
	tasks map[string]ResultChan
}

type CoordinatorResultCache struct {
	mu    sync.Mutex
	cache map[string]NonceCacheEntry
}

type NonceCacheEntry struct {
	NumTrailingZeros uint
	Secret           []uint8
}

func NewCoordinator(config CoordinatorConfig) *Coordinator {
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: "coordinator",
		Secret:         config.TracerSecret,
	})

	workerClients := make([]*WorkerClient, len(config.Workers))
	for i, addr := range config.Workers {
		workerClients[i] = &WorkerClient{
			addr:       addr,
			client:     nil,
			workerByte: uint8(i),
		}
	}

	return &Coordinator{
		config:  config,
		tracer:  tracer,
		workers: workerClients,
	}
}

// Mine is a blocking RPC from powlib instructing the Coordinator to solve a specific pow instance
func (c *CoordRPCHandler) Mine(args CoordMineArgs, reply *CoordMineResponse) error {

	// Create trace from token
	trace := c.tracer.ReceiveToken(args.Token)

	trace.RecordAction(CoordinatorMine{
		NumTrailingZeros: args.NumTrailingZeros,
		Nonce:            args.Nonce,
	})

	// check cache first, return it contains result
	cacheSecret := c.resultCache.cacheGet(args.Nonce, args.NumTrailingZeros, trace)

	if cacheSecret != nil {

		trace.RecordAction(CoordinatorSuccess{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			Secret:           cacheSecret,
		})

		reply.NumTrailingZeros = args.NumTrailingZeros
		reply.Nonce = args.Nonce
		reply.Secret = cacheSecret
		reply.Token = trace.GenerateToken()

		return nil
	}

	// initialize and connect to workers (if not already connected)
	for err := initializeWorkers(c.workers); err != nil; {
		log.Println(err)
		err = initializeWorkers(c.workers)
	}

	workerCount := len(c.workers)

	resultChan := make(chan CoordResultArgs, workerCount*2)
	c.mineTasks.set(args.Nonce, args.NumTrailingZeros, resultChan)

	for _, w := range c.workers {

		trace.RecordAction(CoordinatorWorkerMine{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
		})

		rpcArgs := WorkerMineArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
			WorkerBits:       c.workerBits,
			Token:		      trace.GenerateToken(),
		}

		err := w.client.Call("WorkerRPCHandler.Mine", rpcArgs, &struct{}{})
		if err != nil {
			return err
		}
	}

	// wait for at least one result
	result := <-resultChan
	// sanity check
	if result.Secret == nil {
		log.Fatalf("First worker result appears to be cancellation ACK, from workerByte = %d", result.WorkerByte)
	}

	// after receiving one result, cancel all workers unconditionally.
	// the cancellation takes place of an ACK for any workers sending results.
	for _, w := range c.workers {

		trace.RecordAction(CoordinatorWorkerCancel{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
		})

		rpcArgs := WorkerFoundArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
			Secret:			  result.Secret,
			Token:			  trace.GenerateToken(),

		}
		err := w.client.Call("WorkerRPCHandler.Found", rpcArgs, &struct{}{})
		if err != nil {
			return err
		}
	}

	log.Printf("Waiting for %d acks from workers, then we are done", workerCount)

	// wait for all all workers to send back cancel ACK, ignoring results (receiving them is logged, but they have no further use here)
	// we asked all workers to cancel, so we should get exactly workerCount ACKs.

	workerAcksReceived := 1
	var resultAcks []CoordResultArgs
	for workerAcksReceived < workerCount*2 {
		ack := <-resultChan
		if ack.Secret != nil {
			resultAcks = append(resultAcks, ack)
			log.Printf("WorkerResult ACK: %v", ack)
		} else {
			log.Printf("Cancellation ACK: %v", ack)
		}
		workerAcksReceived += 1
	}

	for _, ack := range resultAcks {
		// Update caches at workers
		for _, w := range c.workers {

			trace.RecordAction(CoordinatorWorkerCancel{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				WorkerByte:       w.workerByte,
			})

			rpcArgs := WorkerFoundArgs{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				WorkerByte:       w.workerByte,
				Secret:			  ack.Secret,
				Token:			  trace.GenerateToken(),
			}

			err := w.client.Call("WorkerRPCHandler.Found", rpcArgs, &struct{}{})
			if err != nil {
				return err
			}
		}
		// Confirm ACKS from workers after updating their caches
		workerCacheAcksReceived := 0
		for workerCacheAcksReceived < workerCount {
			cacheAck := <-resultChan
			log.Printf("cacheACK: %v", cacheAck)
			workerCacheAcksReceived += 1
		}
	}

	// delete completed mine task from map
	c.mineTasks.delete(args.Nonce, args.NumTrailingZeros)

	trace.RecordAction(CoordinatorSuccess{
		Nonce:            result.Nonce,
		NumTrailingZeros: result.NumTrailingZeros,
		Secret:           result.Secret,
	})

	reply.NumTrailingZeros = result.NumTrailingZeros
	reply.Nonce = result.Nonce
	reply.Secret = result.Secret
	// TODO: figure out which trace to use
	reply.Token = trace.GenerateToken()

	return nil
}

// Result is a non-blocking RPC from the worker that sends the solution to some previous pow instance assignment
// back to the Coordinator
func (c *CoordRPCHandler) Result(args CoordResultArgs, reply *struct{}) error {

	// Get token from trace
	trace := c.tracer.ReceiveToken(args.Token)

	if args.Secret != nil {
		trace.RecordAction(CoordinatorWorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           args.Secret,
		})
		c.resultCache.cacheAdd(args.Nonce, args.NumTrailingZeros, args.Secret, trace)
	} else {
		log.Printf("Received worker cancel ack: %v", args)
	}
	c.mineTasks.get(args.Nonce, args.NumTrailingZeros) <- args
	return nil
}

func (c *Coordinator) InitializeRPCs() error {
	handler := &CoordRPCHandler{
		tracer:     c.tracer,
		workers:    c.workers,
		workerBits: uint(math.Log2(float64(len(c.workers)))),
		mineTasks: CoordinatorMineTasks{
			tasks: make(map[string]ResultChan),
		},
		resultCache: CoordinatorResultCache{
			cache: make(map[string]NonceCacheEntry),
		},
	}
	server := rpc.NewServer()
	err := server.Register(handler) // publish Coordinator<->worker procs
	if err != nil {
		return fmt.Errorf("format of Coordinator RPCs aren't correct: %s", err)
	}

	workerListener, e := net.Listen("tcp", c.config.WorkerAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.WorkerAPIListenAddr, e)
	}

	clientListener, e := net.Listen("tcp", c.config.ClientAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.ClientAPIListenAddr, e)
	}

	go server.Accept(workerListener)
	server.Accept(clientListener)

	return nil
}

func initializeWorkers(workers []*WorkerClient) error {
	for _, w := range workers {
		if w.client == nil {
			client, err := rpc.Dial("tcp", string(w.addr))
			if err != nil {
				log.Printf("Waiting for worker %d", w.workerByte)
				return fmt.Errorf("failed to dial worker: %s", err)
			}
			w.client = client
		}
	}
	return nil
}

func (t *CoordinatorMineTasks) get(nonce []uint8, numTrailingZeros uint) ResultChan {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)]
}

func (t *CoordinatorMineTasks) set(nonce []uint8, numTrailingZeros uint, val ResultChan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)] = val
	log.Printf("New task added: %v\n", t.tasks)
}

func (t *CoordinatorMineTasks) delete(nonce []uint8, numTrailingZeros uint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, generateCoordTaskKey(nonce, numTrailingZeros))
	log.Printf("Task deleted: %v\n", t.tasks)
}

//**Functions for accessing cache**//
func (t *CoordinatorResultCache) cacheGet(nonce []uint8, numTrailingZeros uint, trace *tracing.Trace) []uint8 {
	t.mu.Lock()
	defer t.mu.Unlock()
	cacheEntry, found := t.cache[generateCoordCacheKey(nonce)]
	
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

func (t *CoordinatorResultCache) cacheAdd(nonce []uint8, numTrailingZeros uint, secret []uint8, trace *tracing.Trace) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cacheEntry, found := t.cache[generateCoordCacheKey(nonce)]
	if found == false {
		// No entry exists
		t.cache[generateCoordCacheKey(nonce)] = NonceCacheEntry{
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
		delete(t.cache, generateCoordCacheKey(nonce))

		trace.RecordAction(CacheAdd{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		})
		t.cache[generateCoordCacheKey(nonce)] = NonceCacheEntry{
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
		delete(t.cache, generateCoordCacheKey(nonce))

		trace.RecordAction(CacheAdd{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		})
		t.cache[generateCoordCacheKey(nonce)] = NonceCacheEntry{
			NumTrailingZeros: numTrailingZeros,
			Secret:           secret,
		}
	}
}

func generateCoordTaskKey(nonce []uint8, numTrailingZeros uint) string {
	return fmt.Sprintf("%s|%d", hex.EncodeToString(nonce), numTrailingZeros)
}

func generateCoordCacheKey(nonce []uint8) string {
	return string(nonce)
}
