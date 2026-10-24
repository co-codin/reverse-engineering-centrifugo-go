package engine

import (
	"encoding/json"
	"testing"
)

func raw(s string) json.RawMessage { return json.RawMessage(s) }

func TestHistoryPaging(t *testing.T) {
	eng := New()
	opts := StreamOptions{HistorySize: 10}
	for n := 1; n <= 5; n++ {
		published := eng.Publish("room", opts, PublishInput{Data: raw(`{"n":` + itoa(n) + `}`)}, int64(n))
		if published.Offset != uint64(n) {
			t.Fatalf("offset %d, want %d", published.Offset, n)
		}
	}
	latest := eng.History("room", opts, HistoryQuery{Limit: 2}, 0)
	if !latest.OK || offsets(latest.Publications) != "4,5" {
		t.Fatalf("latest %#v", latest.Publications)
	}
	after := eng.History("room", opts, HistoryQuery{Limit: 2, Since: &Position{Offset: 2}}, 0)
	if !after.OK || offsets(after.Publications) != "3,4" {
		t.Fatalf("after %#v", after.Publications)
	}
	backward := eng.History("room", opts, HistoryQuery{Limit: 2, Reverse: true, Since: &Position{Offset: 4}}, 0)
	if !backward.OK || offsets(backward.Publications) != "3,2" {
		t.Fatalf("backward %#v", backward.Publications)
	}
}

func TestRecoveryGap(t *testing.T) {
	eng := New()
	opts := StreamOptions{HistorySize: 2}
	for _, n := range []int{1, 2, 3} {
		eng.Publish("room", opts, PublishInput{Data: raw(`{"n":` + itoa(n) + `}`)}, int64(n))
	}
	missed := eng.Recover("room", opts, "", 0, 0)
	if missed.Recovered || missed.Offset != 3 {
		t.Fatalf("missed %+v", missed)
	}
	caught := eng.Recover("room", opts, "", 1, 0)
	if !caught.Recovered || caught.Offset != 1 || offsets(caught.Publications) != "2,3" {
		t.Fatalf("caught %+v %s", caught, offsets(caught.Publications))
	}
	current := eng.Recover("room", opts, caught.Epoch, 3, 0)
	if !current.Recovered || len(current.Publications) != 0 {
		t.Fatalf("current %+v", current)
	}
	other := eng.Recover("room", opts, "nope", 1, 0)
	if other.Recovered {
		t.Fatal("other epoch recovered")
	}
}

func TestExpiryAndRemove(t *testing.T) {
	eng := New()
	opts := StreamOptions{HistorySize: 10, HistoryTTLMs: 1000}
	eng.Publish("room", opts, PublishInput{Data: raw(`{"n":1}`)}, 0)
	expired := eng.Recover("room", opts, "", 0, 1001)
	if expired.Recovered || expired.Offset != 1 {
		t.Fatalf("expired %+v", expired)
	}
	eng.Publish("room", opts, PublishInput{Data: raw(`{"n":2}`)}, 2000)
	eng.RemoveHistory("room")
	history := eng.History("room", opts, HistoryQuery{Limit: 10}, 2000)
	if !history.OK || len(history.Publications) != 0 || history.Offset != 2 {
		t.Fatalf("history %+v", history)
	}
	bad := eng.History("room", opts, HistoryQuery{Limit: 1, Reverse: true, Since: &Position{}}, 2000)
	if bad.OK || bad.Code != 107 {
		t.Fatalf("reverse from zero %+v", bad)
	}
}

func TestIdempotency(t *testing.T) {
	eng := New()
	opts := StreamOptions{HistorySize: 10}
	first := eng.Publish("room", opts, PublishInput{Data: raw(`{"n":1}`), IdempotencyKey: "same"}, 1)
	second := eng.Publish("room", opts, PublishInput{Data: raw(`{"n":2}`), IdempotencyKey: "same"}, 2)
	if !second.Duplicate || second.Offset != first.Offset {
		t.Fatalf("duplicate %+v %+v", first, second)
	}
	third := eng.Publish("room", opts, PublishInput{Data: raw(`{"n":3}`)}, 3)
	if third.Offset != 2 {
		t.Fatalf("third offset %d", third.Offset)
	}
}

func offsets(pubs []Publication) string {
	out := ""
	for i, pub := range pubs {
		if i > 0 {
			out += ","
		}
		out += itoa(int(pub.Offset))
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
