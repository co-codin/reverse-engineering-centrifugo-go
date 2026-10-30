// Package token signs and checks HS256 connection JWTs.
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Claims are the connection and subscription fields this node understands.
// A connection token uses channels. A subscription token uses channel.
type Claims struct {
	Sub      string          `json:"sub,omitempty"`
	Exp      int64           `json:"exp,omitempty"`
	Iat      int64           `json:"iat,omitempty"`
	Info     json.RawMessage `json:"info,omitempty"`
	Channels []string        `json:"channels,omitempty"`
	Channel  string          `json:"channel,omitempty"`
}

// Sign builds an HS256 JWT.
func Sign(claims Claims, secret string) (string, error) {
	header, err := encodeJSON(map[string]string{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	body, err := encodeJSON(claims)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(header + "." + body))
	return header + "." + body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// Result is a checked connection token.
type Result struct {
	Claims  Claims
	Expired bool
	// BadRequest is a valid signature whose claims are not usable.
	BadRequest bool
}

// Verify checks a connection token signature and expiry.
func Verify(rawToken, secret string, now time.Time) (Result, error) {
	body, err := verifyPayload(rawToken, secret)
	if err != nil {
		return Result{}, err
	}
	var wire struct {
		Sub      json.RawMessage `json:"sub"`
		Exp      *int64          `json:"exp"`
		Info     json.RawMessage `json:"info"`
		Channels json.RawMessage `json:"channels"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return Result{BadRequest: true}, nil
	}
	var claims Claims
	if len(wire.Sub) > 0 && string(wire.Sub) != "null" {
		if err := json.Unmarshal(wire.Sub, &claims.Sub); err != nil || len(claims.Sub) > 128 {
			return Result{BadRequest: true}, nil
		}
	}
	if wire.Exp != nil {
		claims.Exp = *wire.Exp
		if claims.Exp < now.Unix() {
			return Result{Expired: true}, nil
		}
	}
	if len(wire.Info) > 0 && string(wire.Info) != "null" {
		if !json.Valid(wire.Info) {
			return Result{BadRequest: true}, nil
		}
		claims.Info = wire.Info
	}
	if len(wire.Channels) > 0 && string(wire.Channels) != "null" {
		if err := json.Unmarshal(wire.Channels, &claims.Channels); err != nil {
			return Result{BadRequest: true}, nil
		}
	}
	return Result{Claims: claims}, nil
}

// VerifySubscription checks a channel token. The channel claim is required.
func VerifySubscription(rawToken, secret string, now time.Time) (Result, error) {
	body, err := verifyPayload(rawToken, secret)
	if err != nil {
		return Result{}, err
	}
	var wire struct {
		Sub     json.RawMessage `json:"sub"`
		Channel json.RawMessage `json:"channel"`
		Exp     *int64          `json:"exp"`
		Info    json.RawMessage `json:"info"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return Result{BadRequest: true}, nil
	}
	var claims Claims
	if len(wire.Sub) > 0 && string(wire.Sub) != "null" {
		if err := json.Unmarshal(wire.Sub, &claims.Sub); err != nil || len(claims.Sub) > 128 {
			return Result{BadRequest: true}, nil
		}
	}
	if len(wire.Channel) == 0 || string(wire.Channel) == "null" {
		return Result{BadRequest: true}, nil
	}
	if err := json.Unmarshal(wire.Channel, &claims.Channel); err != nil || claims.Channel == "" || len(claims.Channel) > 255 {
		return Result{BadRequest: true}, nil
	}
	if wire.Exp != nil {
		claims.Exp = *wire.Exp
		if claims.Exp < now.Unix() {
			return Result{Expired: true, Claims: claims}, nil
		}
	}
	if len(wire.Info) > 0 && string(wire.Info) != "null" {
		if !json.Valid(wire.Info) {
			return Result{BadRequest: true}, nil
		}
		claims.Info = wire.Info
	}
	return Result{Claims: claims}, nil
}

func verifyPayload(rawToken, secret string) ([]byte, error) {
	parts := strings.Split(rawToken, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, errors.New("unauthorized")
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, errors.New("unauthorized")
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil || header.Alg != "HS256" {
		return nil, errors.New("unauthorized")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, errors.New("unauthorized")
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("unauthorized")
	}
	return body, nil
}

func encodeJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}
