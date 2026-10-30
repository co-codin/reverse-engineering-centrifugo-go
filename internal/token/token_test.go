package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSignAndVerify(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signed, err := Sign(Claims{
		Sub:      "alice",
		Exp:      now.Add(time.Hour).Unix(),
		Info:     json.RawMessage(`{"color":"green"}`),
		Channels: []string{"chat:lobby"},
	}, "dev-secret")
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify(signed, "dev-secret", now)
	if err != nil || got.Expired || got.BadRequest {
		t.Fatalf("%+v %v", got, err)
	}
	if got.Claims.Sub != "alice" || string(got.Claims.Info) != `{"color":"green"}` {
		t.Fatalf("%+v", got.Claims)
	}
	if len(got.Claims.Channels) != 1 || got.Claims.Channels[0] != "chat:lobby" {
		t.Fatalf("%+v", got.Claims.Channels)
	}
}

func TestExpiredToken(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signed, err := Sign(Claims{Sub: "alice", Exp: now.Add(-time.Second).Unix()}, "dev-secret")
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify(signed, "dev-secret", now)
	if err != nil || !got.Expired || got.BadRequest {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestRejectsBadTokens(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signed, err := Sign(Claims{Sub: "alice", Exp: now.Add(time.Hour).Unix()}, "dev-secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(signed, "other-secret", now); err == nil {
		t.Fatal("accepted a token signed with a different secret")
	}
	if _, err := Verify("not-a-token", "dev-secret", now); err == nil {
		t.Fatal("accepted a malformed token")
	}
	parts := strings.Split(signed, ".")
	none := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	if _, err := Verify(none+"."+parts[1]+"."+parts[2], "dev-secret", now); err == nil {
		t.Fatal("accepted alg none")
	}

	badSub := signRaw(`{"alg":"HS256","typ":"JWT"}`, `{"sub":1}`, "dev-secret")
	got, err := Verify(badSub, "dev-secret", now)
	if err != nil || !got.BadRequest {
		t.Fatalf("bad sub %+v %v", got, err)
	}
	longSub := signRaw(`{"alg":"HS256","typ":"JWT"}`, `{"sub":"`+strings.Repeat("a", 129)+`"}`, "dev-secret")
	got, err = Verify(longSub, "dev-secret", now)
	if err != nil || !got.BadRequest {
		t.Fatalf("long sub %+v %v", got, err)
	}
	broken := signRaw(`{"alg":"HS256","typ":"JWT"}`, `{"sub":`, "dev-secret")
	got, err = Verify(broken, "dev-secret", now)
	if err != nil || !got.BadRequest {
		t.Fatalf("broken claims %+v %v", got, err)
	}
}

func TestSubscriptionToken(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signed, err := Sign(Claims{
		Sub:     "alice",
		Channel: "chat:secret",
		Exp:     now.Add(time.Minute).Unix(),
		Info:    json.RawMessage(`{"role":"member"}`),
	}, "dev-secret")
	if err != nil {
		t.Fatal(err)
	}
	got, err := VerifySubscription(signed, "dev-secret", now)
	if err != nil || got.Expired || got.BadRequest {
		t.Fatalf("%+v %v", got, err)
	}
	if got.Claims.Sub != "alice" || got.Claims.Channel != "chat:secret" || string(got.Claims.Info) != `{"role":"member"}` {
		t.Fatalf("%+v", got.Claims)
	}
	connection, err := Verify(signed, "dev-secret", now)
	if err != nil || connection.Claims.Channel != "" {
		t.Fatalf("connection verify %+v %v", connection, err)
	}
	missing, err := VerifySubscription(signRaw(`{"alg":"HS256","typ":"JWT"}`, `{"sub":"alice"}`, "dev-secret"), "dev-secret", now)
	if err != nil || !missing.BadRequest {
		t.Fatalf("missing channel %+v %v", missing, err)
	}
}

func signRaw(headerJSON, bodyJSON, secret string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(headerJSON))
	body := base64.RawURLEncoding.EncodeToString([]byte(bodyJSON))
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(header + "." + body))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return header + "." + body + "." + sig
}
