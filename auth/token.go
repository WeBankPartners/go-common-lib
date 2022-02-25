package auth

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang-jwt/jwt"
	"strings"
	"time"
)

var ErrInvalidClaim = errors.New("invalid claim")

type AuthClaims struct {
	Subject   string `json:"sub"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	Type      string `json:"type"`
	LoginType string `json:"loginType"`
	Account   string `json:"account,omitempty"`
}

func (c AuthClaims) Valid() error {
	now := time.Now().UTC()
	exp := time.Unix(c.ExpiresAt, 0).UTC()
	if now.After(exp) {
		return ErrInvalidClaim
	}

	iat := time.Unix(c.IssuedAt, 0).UTC()
	if now.Before(iat) {
		return ErrInvalidClaim
	}
	return nil
}

func VerifyToken(tokenString string, jwtPublicKeyBytes, didPublicKeyBytes []byte) (did []string, err error) {
	// parse rsa public key
	parsedKey, parsePublicKeyErr := jwt.ParseRSAPublicKeyFromPEM(jwtPublicKeyBytes)
	if parsePublicKeyErr != nil {
		return did, fmt.Errorf("parse jwt public key fail,%s ", parsePublicKeyErr.Error())
	}
	// parse Claim
	jwtToken, parseClaimErr := jwt.ParseWithClaims(tokenString, &AuthClaims{}, func(token *jwt.Token) (interface{}, error) {
		return parsedKey, nil
	})
	if parseClaimErr != nil {
		return did, fmt.Errorf("parse jwt claim fail,%s ", parseClaimErr.Error())
	}
	claim, ok := jwtToken.Claims.(*AuthClaims)
	if !ok || !jwtToken.Valid {
		return did, fmt.Errorf("jwt token invalid ")
	}
	jwtContent, base64DecodeErr := base64.StdEncoding.DecodeString(claim.Account)
	if base64DecodeErr != nil {
		return did, fmt.Errorf("base64 decode public key fail,%s ", base64DecodeErr.Error())
	}
	didContent, decryptDidErr := RSADecryptByPublic(jwtContent, string(didPublicKeyBytes))
	if decryptDidErr != nil {
		return did, fmt.Errorf("decode did by public key fail,%s ", decryptDidErr.Error())
	}
	var didList []string
	err = json.Unmarshal(didContent, &didList)
	if err != nil {
		return did, fmt.Errorf("json unmarshal jwt decode content fail:%s ", err.Error())
	}
	for _, v := range didList {
		if strings.HasPrefix(v, "nonce") {
			continue
		}
		did = append(did, v)
	}
	return
}
