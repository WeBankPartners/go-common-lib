package file_server

import (
	"context"
	"fmt"
	"github.com/WeBankPartners/go-common-lib/guid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"io/ioutil"
	"os"
)

type MinioServer struct {
	ServerAddress string        `json:"server_address"`
	AccessKey     string        `json:"access_key"`
	SecretKey     string        `json:"secret_key"`
	SSL           bool          `json:"ssl"`
	Client        *minio.Client `json:"client"`
}

type MinioUploadParam struct {
	Ctx        context.Context `json:"ctx"`
	Bucket     string          `json:"bucket"`
	ObjectName string          `json:"object_name"`
	Reader     io.Reader       `json:"reader"`
	ObjectSize int64           `json:"object_size"`
}

type MinioDownloadParam struct {
	Ctx        context.Context `json:"ctx"`
	Bucket     string          `json:"bucket"`
	ObjectName string          `json:"object_name"`
	FilePath   string          `json:"file_path"`
}

func (m *MinioServer) Init() error {
	minioClient, err := minio.New(m.ServerAddress, &minio.Options{
		Creds:  credentials.NewStaticV4(m.AccessKey, m.SecretKey, ""),
		Secure: m.SSL,
	})
	if err == nil {
		m.Client = minioClient
	}
	return err
}

func (m *MinioServer) Upload(param MinioUploadParam) error {
	err := m.Client.MakeBucket(param.Ctx, param.Bucket, minio.MakeBucketOptions{Region: ""})
	if err != nil {
		exists, errBucketExists := m.Client.BucketExists(param.Ctx, param.Bucket)
		if errBucketExists != nil {
			return fmt.Errorf("Check bucket if exist fail,%s ", errBucketExists.Error())
		}
		if !exists {
			return fmt.Errorf("Bucket:%s is not exist ", param.Bucket)
		}
	}
	info, putErr := m.Client.PutObject(param.Ctx, param.Bucket, param.ObjectName, param.Reader, param.ObjectSize, minio.PutObjectOptions{})
	if putErr != nil {
		return fmt.Errorf("Upload minio file fail,%s ", putErr.Error())
	}
	fmt.Printf("info key:%s \n", info.Key)
	return nil
}

func (m *MinioServer) Download(param MinioDownloadParam) (result []byte, err error) {
	tmpPath := param.FilePath
	if tmpPath == "" {
		tmpPath = fmt.Sprintf("/tmp/tmp_s3_%s", guid.CreateGuid())
	}
	err = m.Client.FGetObject(param.Ctx, param.Bucket, param.ObjectName, tmpPath, minio.GetObjectOptions{})
	if err != nil {
		os.Remove(tmpPath)
		err = fmt.Errorf("Download minio file fail,%s ", err.Error())
		return result, err
	}
	if param.FilePath == "" {
		result, _ = ioutil.ReadFile(tmpPath)
		os.Remove(tmpPath)
	}
	return result, err
}
