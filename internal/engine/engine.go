// Package engine is the in-memory channel history for one node.
package engine

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"time"
)

const idempotencyTTL = 5 * time.Minute

// StreamOptions is the history policy for a namespace.
type StreamOptions struct {
	HistorySize  int
	HistoryTTLMs int64
}

// Publication is one stored or live message.
type Publication struct {
	Data   json.RawMessage
	Offset uint64
	At     int64
	Info   map[string]any
	Tags   map[string]string
}

// Wire is the publication object sent to clients.
func (p Publication) Wire() map[string]any {
	out := map[string]any{"data": p.Data}
	if p.Offset > 0 {
		out["offset"] = p.Offset
	}
	if len(p.Info) > 0 {
		out["info"] = p.Info
	}
	if len(p.Tags) > 0 {
		out["tags"] = p.Tags
	}
	return out
}

// PublishInput is one publish into a channel.
type PublishInput struct {
	Data           json.RawMessage
	SkipHistory    bool
	Info           map[string]any
	Tags           map[string]string
	IdempotencyKey string
}

// PublishOutcome is the stream position of a publish.
type PublishOutcome struct {
	Duplicate   bool
	Epoch       string
	HasEpoch    bool
	Offset      uint64
	HasOffset   bool
	Publication *Publication
}

// Recovery is the result of catching a subscriber up.
type Recovery struct {
	Recovered    bool
	Epoch        string
	Offset       uint64
	Publications []Publication
}

// HistoryQuery selects a slice of history.
type HistoryQuery struct {
	Limit   int
	Reverse bool
	Since   *Position
}

// Position is an epoch and offset.
type Position struct {
	Offset uint64
	Epoch  string
}

// HistoryOutcome is history or a protocol error.
type HistoryOutcome struct {
	OK           bool
	Code         int
	Message      string
	Epoch        string
	Offset       uint64
	Publications []Publication
}

type stream struct {
	epoch  string
	offset uint64
	pubs   []Publication
}

type cachedPublish struct {
	exp       int64
	epoch     string
	hasEpoch  bool
	offset    uint64
	hasOffset bool
}

// Engine stores channel streams for the life of the process.
type Engine struct {
	streams     map[string]*stream
	idempotency map[string]map[string]cachedPublish
}

// New returns an empty engine.
func New() *Engine {
	return &Engine{
		streams:     map[string]*stream{},
		idempotency: map[string]map[string]cachedPublish{},
	}
}

// Publish appends data to a channel stream and remembers idempotency keys.
func (e *Engine) Publish(channel string, options StreamOptions, input PublishInput, now int64) PublishOutcome {
	if input.IdempotencyKey != "" {
		if cached, ok := e.readIdempotency(channel, input.IdempotencyKey, now); ok {
			return PublishOutcome{
				Duplicate: true,
				Epoch:     cached.epoch,
				HasEpoch:  cached.hasEpoch,
				Offset:    cached.offset,
				HasOffset: cached.hasOffset,
			}
		}
	}

	store := !input.SkipHistory && options.HistorySize > 0
	var outcome PublishOutcome
	publication := Publication{Data: input.Data, At: now, Info: input.Info, Tags: input.Tags}
	if store {
		st := e.ensure(channel)
		e.prune(st, options, now)
		st.offset++
		publication.Offset = st.offset
		st.pubs = append(st.pubs, publication)
		e.prune(st, options, now)
		outcome.Epoch = st.epoch
		outcome.HasEpoch = true
		outcome.Offset = st.offset
		outcome.HasOffset = true
	}
	outcome.Publication = &publication
	if input.IdempotencyKey != "" {
		e.remember(channel, input.IdempotencyKey, outcome, now)
	}
	return outcome
}

// Position returns the current stream top, creating the stream if needed.
func (e *Engine) Position(channel string, options StreamOptions, now int64) (string, uint64) {
	st := e.ensure(channel)
	e.prune(st, options, now)
	return st.epoch, st.offset
}

// Recover returns publications the subscriber missed after sinceOffset.
// On success Offset is the position the client already has.
// On a gap or an epoch change Offset is the current stream top and Recovered is false.
// An empty epoch means the current epoch.
func (e *Engine) Recover(channel string, options StreamOptions, sinceEpoch string, sinceOffset uint64, now int64) Recovery {
	st := e.ensure(channel)
	e.prune(st, options, now)
	missed := Recovery{Epoch: st.epoch, Offset: st.offset}
	if sinceEpoch != "" && sinceEpoch != st.epoch {
		return missed
	}
	if sinceOffset > st.offset {
		return missed
	}
	if sinceOffset == st.offset {
		return Recovery{Recovered: true, Epoch: st.epoch, Offset: st.offset}
	}
	if len(st.pubs) == 0 || st.pubs[0].Offset > sinceOffset+1 {
		return missed
	}
	var publications []Publication
	for _, pub := range st.pubs {
		if pub.Offset > sinceOffset {
			publications = append(publications, pub)
		}
	}
	for i, pub := range publications {
		if pub.Offset != sinceOffset+uint64(i)+1 {
			return missed
		}
	}
	if len(publications) == 0 || publications[len(publications)-1].Offset != st.offset {
		return missed
	}
	return Recovery{Recovered: true, Epoch: st.epoch, Offset: sinceOffset, Publications: publications}
}

// History returns a page of the stream.
func (e *Engine) History(channel string, options StreamOptions, query HistoryQuery, now int64) HistoryOutcome {
	st := e.ensure(channel)
	e.prune(st, options, now)
	if query.Since != nil && query.Since.Epoch != "" && query.Since.Epoch != st.epoch {
		return HistoryOutcome{Code: 112, Message: "unrecoverable position"}
	}
	if query.Reverse && query.Since != nil && query.Since.Offset == 0 {
		return HistoryOutcome{Code: 107, Message: "bad request"}
	}
	pubs := st.pubs
	if query.Since != nil {
		filtered := pubs[:0:0]
		for _, pub := range pubs {
			if query.Reverse && pub.Offset < query.Since.Offset {
				filtered = append(filtered, pub)
			}
			if !query.Reverse && pub.Offset > query.Since.Offset {
				filtered = append(filtered, pub)
			}
		}
		pubs = filtered
	}
	switch {
	case query.Limit <= 0:
		pubs = nil
	case query.Reverse:
		pubs = reversed(pubs)
		if len(pubs) > query.Limit {
			pubs = pubs[:query.Limit]
		}
	case query.Since != nil:
		if len(pubs) > query.Limit {
			pubs = pubs[:query.Limit]
		}
	default:
		if len(pubs) > query.Limit {
			pubs = pubs[len(pubs)-query.Limit:]
		}
	}
	return HistoryOutcome{OK: true, Epoch: st.epoch, Offset: st.offset, Publications: pubs}
}

// RemoveHistory drops stored publications and keeps the stream position.
func (e *Engine) RemoveHistory(channel string) {
	st := e.streams[channel]
	if st == nil {
		return
	}
	st.pubs = nil
}

func (e *Engine) ensure(channel string) *stream {
	st := e.streams[channel]
	if st == nil {
		st = &stream{epoch: newEpoch()}
		e.streams[channel] = st
	}
	return st
}

func (e *Engine) prune(st *stream, options StreamOptions, now int64) {
	if options.HistoryTTLMs > 0 {
		cutoff := now - options.HistoryTTLMs
		kept := st.pubs[:0:0]
		for _, pub := range st.pubs {
			if pub.At >= cutoff {
				kept = append(kept, pub)
			}
		}
		st.pubs = kept
	}
	if len(st.pubs) > options.HistorySize {
		st.pubs = st.pubs[len(st.pubs)-options.HistorySize:]
	}
}

func (e *Engine) readIdempotency(channel, key string, now int64) (cachedPublish, bool) {
	bucket := e.idempotency[channel]
	found, ok := bucket[key]
	if !ok {
		return cachedPublish{}, false
	}
	if found.exp <= now {
		delete(bucket, key)
		return cachedPublish{}, false
	}
	return found, true
}

func (e *Engine) remember(channel, key string, outcome PublishOutcome, now int64) {
	bucket := e.idempotency[channel]
	if bucket == nil {
		bucket = map[string]cachedPublish{}
		e.idempotency[channel] = bucket
	}
	for cachedKey, entry := range bucket {
		if entry.exp <= now {
			delete(bucket, cachedKey)
		}
	}
	bucket[key] = cachedPublish{
		exp:       now + idempotencyTTL.Milliseconds(),
		epoch:     outcome.Epoch,
		hasEpoch:  outcome.HasEpoch,
		offset:    outcome.Offset,
		hasOffset: outcome.HasOffset,
	}
}

func newEpoch() string {
	var buf [6]byte
	_, _ = rand.Read(buf[:])
	return base64.RawURLEncoding.EncodeToString(buf[:])
}

func reversed(pubs []Publication) []Publication {
	out := make([]Publication, len(pubs))
	for i := range pubs {
		out[len(pubs)-1-i] = pubs[i]
	}
	return out
}
