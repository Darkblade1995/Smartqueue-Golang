package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"
)

type APIKey struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	KeyHash   string    `json:"-"`         
	KeyPrefix string    `json:"key_prefix"` 
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}


type APIKeyResponse struct {
	APIKey
	Key string `json:"key"` 
}


func GenerateKey() (key string, hash string, prefix string, err error) {
	// 32 bytes aleatorios
	bytes := make([]byte, 32)
	if _, err = rand.Read(bytes); err != nil {
		return "", "", "", fmt.Errorf("error generando key: %w", err)
	}


	key = "sk_live_" + base64.URLEncoding.EncodeToString(bytes)


	hash = hashKey(key)


	prefix = key[:16]

	return key, hash, prefix, nil
}


func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x", h)
}