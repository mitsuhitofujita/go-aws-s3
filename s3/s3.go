package s3

import (
	"crypto/md5" //nolint:gosec
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	client "github.com/aws/aws-sdk-go/service/s3"
	manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const megabyte = 1024 * 1024

type UploaderInterface interface {
	upload(s3 *S3, filePath, s3Path string) (out *manager.UploadOutput, err error)
}

type DownloaderInterface interface {
	download(s3 *S3, filePath, s3Path string) (n int64, err error)
}

type HeadGetterInterface interface {
	get(s3 *S3, s3Path string) (out *client.HeadObjectOutput, err error)
}

type HashMakerInterface interface {
	makeMultiPartFromFile(filePath string, partSize int) (hash string, err error)
	makeSinglePartFromFile(filePath string) (hash string, err error)
}

type S3 struct {
	credentials  *credentials.Credentials
	session      *session.Session
	region       *string
	bucket       *string
	s3           *client.S3
	retryCnt     int
	waitDuration time.Duration
	uploader     UploaderInterface
	downloader   DownloaderInterface
	headGetter   HeadGetterInterface
	hashMaker    HashMakerInterface
}

type Config struct {
	ID           string
	Secret       string
	Token        string
	Region       string
	Bucket       string
	RetryCnt     int
	WaitDuration time.Duration
	Uploader     UploaderInterface
	Downloader   DownloaderInterface
	HeadGetter   HeadGetterInterface
	HashMaker    HashMakerInterface
}

func New(config *Config) (s3 *S3, err error) {
	config.setDefault()

	creds := credentials.NewStaticCredentials(config.ID, config.Secret, config.Token)
	region := aws.String(config.Region)
	bucket := aws.String(config.Bucket)
	sess, err := session.NewSession(&aws.Config{
		Credentials: creds,
		Region:      region,
	})
	if err != nil {
		return
	}
	s3 = &S3{
		credentials:  creds,
		session:      sess,
		region:       region,
		bucket:       bucket,
		s3:           client.New(sess),
		retryCnt:     config.RetryCnt,
		waitDuration: config.WaitDuration,
		uploader:     config.Uploader,
		downloader:   config.Downloader,
		headGetter:   config.HeadGetter,
		hashMaker:    config.HashMaker,
	}
	return
}

func (config *Config) setDefault() {
	if config.RetryCnt <= 0 {
		config.RetryCnt = 3
	}
	if config.WaitDuration <= 0 {
		config.WaitDuration = 3
	}
	if config.Uploader == nil {
		config.Uploader = &Uploader{}
	}
	if config.Downloader == nil {
		config.Downloader = &Downloader{}
	}
	if config.HeadGetter == nil {
		config.HeadGetter = &HeadGetter{}
	}
	if config.HashMaker == nil {
		config.HashMaker = &HashMaker{}
	}
}

func (s3 *S3) Upload(filePath, s3Path string) (err error) {
	for i := 0; i < s3.retryCnt; i++ {
		err = nil

		_, err = s3.uploader.upload(s3, filePath, s3Path)
		if err != nil {
			s3.wait()
			continue
		}

		var isETag bool
		isETag, err = s3.IsETag(filePath, s3Path)
		if err != nil {
			s3.wait()
			continue
		}

		if !isETag {
			err = errors.New("unmatch etag")
			s3.wait()
			continue
		}
		break
	}
	return
}

type Uploader struct {
}

func (uploader *Uploader) upload(s3 *S3, filePath, s3Path string) (out *manager.UploadOutput, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer func() {
		e := file.Close()
		if err == nil {
			err = e
		}
	}()

	s3PathStr := aws.String(s3Path)

	s3Uploader := manager.NewUploader(s3.session)
	out, err = s3Uploader.Upload(
		&manager.UploadInput{
			Bucket: s3.bucket,
			Key:    s3PathStr,
			Body:   file,
		},
	)
	if err != nil {
		return
	}

	err = s3.s3.WaitUntilObjectExists(
		&client.HeadObjectInput{
			Bucket: s3.bucket,
			Key:    s3PathStr,
		},
	)
	return out, err
}

func (s3 *S3) Download(filePath, s3Path string) (n int64, err error) {
	for i := 0; i < s3.retryCnt; i++ {
		err = nil

		n, err = s3.downloader.download(s3, filePath, s3Path)
		if err != nil {
			s3.wait()
			continue
		}

		var isETag bool
		isETag, err = s3.IsETag(filePath, s3Path)
		if err != nil {
			s3.wait()
			continue
		}

		if !isETag {
			err = errors.New("unmatch etag")
			s3.wait()
			continue
		}

		break
	}
	return n, err
}

type Downloader struct {
}

func (downloader Downloader) download(s3 *S3, filePath, s3Path string) (n int64, err error) {
	file, err := os.Create(filePath)
	if err != nil {
		return
	}
	defer func() {
		e := file.Close()
		if e != nil {
			e = os.Remove(filePath)
		}
		if err != nil {
			err = e
		}
	}()

	s3Downloader := manager.NewDownloader(s3.session)
	n, err = s3Downloader.Download(
		file,
		&client.GetObjectInput{
			Bucket: s3.bucket,
			Key:    aws.String(s3Path),
		},
	)
	return
}

type HeadGetter struct{}

func (headGetter *HeadGetter) get(s3 *S3, s3Path string) (out *client.HeadObjectOutput, err error) {
	return s3.s3.HeadObject(
		&client.HeadObjectInput{
			Bucket: s3.bucket,
			Key:    aws.String(s3Path),
		},
	)
}

func (s3 *S3) IsETag(filePath, s3Path string) (isETag bool, err error) {
	out, err := s3.headGetter.get(s3, s3Path)
	if err != nil {
		return
	}

	eTag := strings.Trim(*out.ETag, "\" ")
	fileSize := int(*out.ContentLength)

	isETag, err = s3.isETag(filePath, eTag, fileSize)
	return
}

func (s3 *S3) wait() {
	time.Sleep(s3.waitDuration * time.Second)
}

func (s3 *S3) isETag(filePath, eTag string, fileSize int) (isETag bool, err error) {
	hash, partCnt, err := GetETagHashAndPartCnt(eTag)
	if err != nil {
		return
	}

	var fileHash string
	if partCnt > 1 {
		var partSize int
		partSize, err = GetMultiPartSize(fileSize, partCnt)
		if err != nil {
			return
		}

		fileHash, err = s3.hashMaker.makeMultiPartFromFile(filePath, partSize)
	} else {
		fileHash, err = s3.hashMaker.makeSinglePartFromFile(filePath)
	}
	if err != nil {
		return
	}

	isETag = fileHash == hash
	return
}

func GetETagHashAndPartCnt(eTag string) (hash string, partCnt int, err error) {
	splitted := strings.Split(eTag, "-")
	if eTag == "" {
		err = errors.New("empty etag")
		return
	}
	if len(splitted) > 1 {
		hash = splitted[0]
		partCnt, err = strconv.Atoi(splitted[1])
	} else {
		hash = eTag
		partCnt = 1
	}
	return
}

func GetMultiPartSize(fileSize, partCnt int) (partSize int, err error) {
	if partCnt <= 1 {
		err = errors.New("invalid part count")
		return
	}

	mb := partCnt * megabyte
	m := fileSize % mb
	d := fileSize / mb

	if m > 0 {
		d++
	}
	partSize = d * megabyte
	return
}

type HashMaker struct{}

func (hashMaker *HashMaker) makeMultiPartFromFile(filePath string, partSize int) (hash string, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer func() {
		e := file.Close()
		if err == nil {
			err = e
		}
	}()

	b := make([]byte, partSize)
	h := make([]byte, 0, 128)
	for {
		n, e := file.Read(b)
		if e == io.EOF {
			break
		}
		if e != nil {
			err = e
			return
		}
		sum := md5.Sum(b[:n]) //nolint:gosec
		h = append(h, sum[:]...)
	}
	sum := md5.Sum(h) //nolint:gosec
	hash = getMd5FromBytes(sum[:16])
	return
}

func (hashMaker *HashMaker) makeSinglePartFromFile(filePath string) (hash string, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer func() {
		e := f.Close()
		if err == nil {
			err = e
		}
	}()

	h := md5.New() //nolint:gosec
	_, err = io.Copy(h, f)
	if err != nil {
		return
	}
	hash = getMd5FromBytes(h.Sum(nil))
	return
}

func getMd5FromBytes(b []byte) string {
	return hex.EncodeToString(b)
}
