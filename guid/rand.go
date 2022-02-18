package guid

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/satori/go.uuid"
	"sort"
	"time"
)

func CreateGuid() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x%x", uint32(time.Now().Unix()), b[4:8])
}

func CreateGuidList(num int) []string {
	var guidList guidSortList
	for i := 0; i < num; i++ {
		guidList = append(guidList, CreateGuid())
	}
	sort.Sort(guidList)
	return guidList
}

type guidSortList []string

func (l guidSortList) Len() int {
	return len(l)
}

func (l guidSortList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func (l guidSortList) Less(i, j int) bool {
	return l[i] < l[j]
}

func uuidToString(u uuid.UUID, dash bool) string {
	if dash {
		return u.String()
	}
	buf := make([]byte, 32)
	hex.Encode(buf[0:8], u[0:4])
	hex.Encode(buf[8:12], u[4:6])
	hex.Encode(buf[12:16], u[6:8])
	hex.Encode(buf[16:20], u[8:10])
	hex.Encode(buf[20:], u[10:])
	return string(buf)
}

func GenerateUUIDV4() string {
	u := uuid.NewV4()
	return uuidToString(u, false)
}
