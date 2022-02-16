package auth

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"github.com/WeBankPartners/go-common-lib/cipher"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"net/http"
)

const (
	SourceAuthHeaderName = "Source-Auth"
	AppIdHeaderName      = "App-Id"
)

type HttpSourceAuth struct {
	SourceAuth           string            `json:"source_auth"`
	AppId                string            `json:"app_id"`
	RequestURI           string            `json:"request_uri"`
	RequestBodyString    string            `json:"request_body_string"`
	LegalSourcePubKeyMap map[string]string `json:"legal_source_pub_key_map"`
}

func (h *HttpSourceAuth) InitGinRequest(c *gin.Context, pubKeyMap map[string]string) error {
	h.SourceAuth = c.GetHeader(SourceAuthHeaderName)
	h.AppId = c.GetHeader(AppIdHeaderName)
	h.RequestURI = c.Request.RequestURI
	bodyBytes, _ := ioutil.ReadAll(c.Request.Body)
	c.Request.Body.Close()
	c.Request.Body = ioutil.NopCloser(bytes.NewReader(bodyBytes))
	h.RequestBodyString = string(bodyBytes)
	h.LegalSourcePubKeyMap = pubKeyMap
	return h.validateParam()
}

func (h *HttpSourceAuth) InitHttpRequest(httpRequest *http.Request, pubKeyMap map[string]string) error {
	h.SourceAuth = httpRequest.Header.Get(SourceAuthHeaderName)
	h.AppId = httpRequest.Header.Get(AppIdHeaderName)
	h.RequestURI = httpRequest.URL.RequestURI()
	bodyBytes, _ := ioutil.ReadAll(httpRequest.Body)
	httpRequest.Body.Close()
	httpRequest.Body = ioutil.NopCloser(bytes.NewReader(bodyBytes))
	h.RequestBodyString = string(bodyBytes)
	h.LegalSourcePubKeyMap = pubKeyMap
	return h.validateParam()
}

func (h *HttpSourceAuth) validateParam() error {
	if h.SourceAuth == "" || h.AppId == "" {
		return fmt.Errorf("Http header Source-Auth and App-Id can not empty ")
	}
	if len(h.LegalSourcePubKeyMap) == 0 {
		return fmt.Errorf("Source public key list is empty ")
	}
	return nil
}

func (h *HttpSourceAuth) Auth() error {
	var pubKeyString string
	if v, b := h.LegalSourcePubKeyMap[h.AppId]; b {
		pubKeyString = v
	} else {
		return fmt.Errorf("Can not find appId:%s public key ", h.AppId)
	}
	decodeBytes, err := cipher.RSADecryptByPublic([]byte(h.SourceAuth), []byte(pubKeyString))
	if err != nil {
		return err
	}
	if string(decodeBytes) != hashSourceAuthHeader(h.AppId, h.RequestURI, h.RequestBodyString) {
		return fmt.Errorf("Validate sign content fail ")
	}
	return nil
}

func (h *HttpSourceAuth) GetAppId() string {
	return h.AppId
}

func SetRequestSourceAuth(httpRequest *http.Request, appId string, privateKey []byte) error {
	httpRequest.Header.Set(AppIdHeaderName, appId)
	b, _ := ioutil.ReadAll(httpRequest.Body)
	httpRequest.Body.Close()
	httpRequest.Body = ioutil.NopCloser(bytes.NewReader(b))
	enBytes, enErr := cipher.RSAEncryptByPrivate([]byte(hashSourceAuthHeader(appId, httpRequest.URL.RequestURI(), string(b))), privateKey)
	if enErr != nil {
		return fmt.Errorf("Rsa encrypt error:%s \n", enErr.Error())
	}
	httpRequest.Header.Set(SourceAuthHeaderName, base64.StdEncoding.EncodeToString(enBytes))
	return nil
}

func hashSourceAuthHeader(appId, httpURI, requestBodyString string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s%s%s", appId, httpURI, requestBodyString))))
}
