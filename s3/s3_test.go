package s3

import (
	"errors"
	client "github.com/aws/aws-sdk-go/service/s3"
	manager "github.com/aws/aws-sdk-go/service/s3/s3manager"
	"testing"
)

type HeadGetterTest struct {
	Values           []HeadGetterTestValue
	CurrentPosition  int
	ExpectedPosition int
}

type HeadGetterTestValue struct {
	Out client.HeadObjectOutput
	Err error
}

func (hgt *HeadGetterTest) get(s3 *S3, s3Path string) (out *client.HeadObjectOutput, err error) {
	out = &hgt.Values[hgt.CurrentPosition].Out
	err = hgt.Values[hgt.CurrentPosition].Err
	hgt.CurrentPosition++
	return
}

type HashMakerTest struct {
	Values           []HashMakerTestValue
	CurrentPosition  int
	ExpectedPosition int
}

type HashMakerTestValue struct {
	Hash string
	Err  error
}

func (hmt *HashMakerTest) makeMultiPartFromFile(filePath string, partSize int) (hash string, err error) {
	hash = hmt.Values[hmt.CurrentPosition].Hash
	err = hmt.Values[hmt.CurrentPosition].Err
	hmt.CurrentPosition++
	return
}

func (hmt *HashMakerTest) makeSinglePartFromFile(filePath string) (hash string, err error) {
	hash = hmt.Values[hmt.CurrentPosition].Hash
	err = hmt.Values[hmt.CurrentPosition].Err
	hmt.CurrentPosition++
	return
}


type UploaderTest struct {
	Values           []UploaderTestValue
	CurrentPosition  int
	ExpectedPosition int
}

type UploaderTestValue struct {
	Out manager.UploadOutput
	Err error
}

func (ut *UploaderTest) upload(s3 *S3, filePath, s3Path string) (out *manager.UploadOutput, err error) {
	out = &ut.Values[ut.CurrentPosition].Out
	err = ut.Values[ut.CurrentPosition].Err
	ut.CurrentPosition++
	return
}

func TestUpload(t *testing.T) {

	eTag := "  \"1234567890\"  "
	contentLength := int64(123)

	cases := []struct {
		describe   string
		uploader   UploaderTest
		headGetter HeadGetterTest
		hashMaker  HashMakerTest
		err        error
	}{
		{
			"最初アップロードに失敗するが、2回目で成功する",
			UploaderTest{
				[]UploaderTestValue{
					{
						manager.UploadOutput{},
						errors.New("upload error"),
					},
					{
						manager.UploadOutput{},
						nil,
					},
				},
				0,
				2,
			},
			HeadGetterTest{
				[]HeadGetterTestValue{
					{
						client.HeadObjectOutput{
							ETag:          &eTag,
							ContentLength: &contentLength,
						},
						nil,
					},
				},
				0,
				1,
			},
			HashMakerTest{
				[]HashMakerTestValue{
					{
						"1234567890",
						nil,
					},
				},
				0,
				1,
			},
			nil,
		},
		{
			"3回ともエラーとなる",
			UploaderTest{
				[]UploaderTestValue{
					{
						manager.UploadOutput{},
						errors.New("upload error"),
					},
					{
						manager.UploadOutput{},
						nil,
					},
					{
						manager.UploadOutput{},
						nil,
					},
				},
				0,
				3,
			},
			HeadGetterTest{
				[]HeadGetterTestValue{
					{
						client.HeadObjectOutput{},
						errors.New("head object error"),
					},
					{
						client.HeadObjectOutput{
							ETag:          &eTag,
							ContentLength: &contentLength,
						},
						nil,
					},
				},
				0,
				2,
			},
			HashMakerTest{
				[]HashMakerTestValue{
					{
						"",
						errors.New("make hash error"),
					},
				},
				0,
				1,
			},
			errors.New("make hash error"),
		},
		{
			"2回アップロードに失敗して3回目でハッシュが合わずエラーとなる",
			UploaderTest{
				[]UploaderTestValue{
					{
						manager.UploadOutput{},
						errors.New("upload error"),
					},
					{
						manager.UploadOutput{},
						errors.New("upload error"),
					},
					{
						manager.UploadOutput{},
						nil,
					},
				},
				0,
				3,
			},
			HeadGetterTest{
				[]HeadGetterTestValue{
					{
						client.HeadObjectOutput{
							ETag:          &eTag,
							ContentLength: &contentLength,
						},
						nil,
					},
				},
				0,
				1,
			},
			HashMakerTest{
				[]HashMakerTestValue{
					{
						"1111111111",
						nil,
					},
				},
				0,
				1,
			},
			errors.New("unmatch etag"),
		},
	}
	for _, c := range cases {
		s3, err := New(&Config{
			Id:         "ID",
			Uploader:   &c.uploader,
			HeadGetter: &c.headGetter,
			HashMaker:  &c.hashMaker,
		})
		if err != nil {
			t.Errorf("could not create s3")
			return
		}

		err = s3.Upload("filePath", "s3Path")

		// エラーが期待値と一致するか
		if c.err != nil {
			if err != nil {
				if err.Error() != c.err.Error() {
					t.Errorf("%v err expected:%v, actual:%v", c.describe, c.err.Error(), err.Error())
				}
			} else {
				t.Errorf("%v err expected:%v, actual:%v", c.describe, c.err.Error(), nil)
			}
		} else {
			if err != nil {
				t.Errorf("%v err expected:%v, actual:%v", c.describe, nil, err.Error())
			}
		}

		// 関数の呼び出し回数が期待値と一致するか
		if c.uploader.ExpectedPosition != c.uploader.CurrentPosition {
			t.Errorf("%v uploader counter expected:%v, actual:%v", c.describe, c.uploader.ExpectedPosition, c.uploader.CurrentPosition)
		}
		if c.headGetter.ExpectedPosition != c.headGetter.CurrentPosition {
			t.Errorf("%v headGetter counter expected:%v, actual:%v", c.describe, c.headGetter.ExpectedPosition, c.headGetter.CurrentPosition)
		}
		if c.hashMaker.ExpectedPosition != c.hashMaker.CurrentPosition {
			t.Errorf("%v hashMaker counter expected:%v, actual:%v", c.describe, c.hashMaker.ExpectedPosition, c.hashMaker.CurrentPosition)
		}
	}
}


type DownloaderTest struct {
	Values           []DownloaderTestValue
	CurrentPosition  int
	ExpectedPosition int
}

type DownloaderTestValue struct {
	Out int64
	Err error
}

func (dt *DownloaderTest) download(s3 *S3, filePath, s3Path string) (out int64, err error) {
	out = dt.Values[dt.CurrentPosition].Out
	err = dt.Values[dt.CurrentPosition].Err
	dt.CurrentPosition++
	return
}

func TestDownload(t *testing.T) {

	eTag := "  \"1234567890\"  "
	contentLength := int64(123)

	cases := []struct {
		describe   string
		downloader DownloaderTest
		headGetter HeadGetterTest
		hashMaker  HashMakerTest
		err        error
	}{
		{
			"最初ダウンロードに失敗するが、2回目で成功する",
			DownloaderTest{
				[]DownloaderTestValue{
					{
						0,
						errors.New("upload error"),
					},
					{
						99,
						nil,
					},
				},
				0,
				2,
			},
			HeadGetterTest{
				[]HeadGetterTestValue{
					{
						client.HeadObjectOutput{
							ETag:          &eTag,
							ContentLength: &contentLength,
						},
						nil,
					},
				},
				0,
				1,
			},
			HashMakerTest{
				[]HashMakerTestValue{
					{
						"1234567890",
						nil,
					},
				},
				0,
				1,
			},
			nil,
		},
		{
			"3回ともエラーとなる",
			DownloaderTest{
				[]DownloaderTestValue{
					{
						0,
						errors.New("downloader error"),
					},
					{
						99,
						nil,
					},
					{
						99,
						nil,
					},
				},
				0,
				3,
			},
			HeadGetterTest{
				[]HeadGetterTestValue{
					{
						client.HeadObjectOutput{},
						errors.New("head object error"),
					},
					{
						client.HeadObjectOutput{
							ETag:          &eTag,
							ContentLength: &contentLength,
						},
						nil,
					},
				},
				0,
				2,
			},
			HashMakerTest{
				[]HashMakerTestValue{
					{
						"",
						errors.New("make hash error"),
					},
				},
				0,
				1,
			},
			errors.New("make hash error"),
		},
		{
			"2回ダウンロードに失敗して3回目でハッシュが合わずエラーとなる",
			DownloaderTest{
				[]DownloaderTestValue{
					{
						0,
						errors.New("upload error"),
					},
					{
						0,
						errors.New("upload error"),
					},
					{
						99,
						nil,
					},
				},
				0,
				3,
			},
			HeadGetterTest{
				[]HeadGetterTestValue{
					{
						client.HeadObjectOutput{
							ETag:          &eTag,
							ContentLength: &contentLength,
						},
						nil,
					},
				},
				0,
				1,
			},
			HashMakerTest{
				[]HashMakerTestValue{
					{
						"1111111111",
						nil,
					},
				},
				0,
				1,
			},
			errors.New("unmatch etag"),
		},
	}
	for _, c := range cases {
		s3, err := New(&Config{
			Id:         "ID",
			Downloader:   &c.downloader,
			HeadGetter: &c.headGetter,
			HashMaker:  &c.hashMaker,
		})
		if err != nil {
			t.Errorf("could not create s3")
			return
		}

		_, err = s3.Download("filePath", "s3Path")

		// エラーが期待値と一致するか
		if c.err != nil {
			if err != nil {
				if err.Error() != c.err.Error() {
					t.Errorf("%v err expected:%v, actual:%v", c.describe, c.err.Error(), err.Error())
				}
			} else {
				t.Errorf("%v err expected:%v, actual:%v", c.describe, c.err.Error(), nil)
			}
		} else {
			if err != nil {
				t.Errorf("%v err expected:%v, actual:%v", c.describe, nil, err.Error())
			}
		}

		// 関数の呼び出し回数が期待値と一致するか
		if c.downloader.ExpectedPosition != c.downloader.CurrentPosition {
			t.Errorf("%v uploader counter expected:%v, actual:%v", c.describe, c.downloader.ExpectedPosition, c.downloader.CurrentPosition)
		}
		if c.headGetter.ExpectedPosition != c.headGetter.CurrentPosition {
			t.Errorf("%v headGetter counter expected:%v, actual:%v", c.describe, c.headGetter.ExpectedPosition, c.headGetter.CurrentPosition)
		}
		if c.hashMaker.ExpectedPosition != c.hashMaker.CurrentPosition {
			t.Errorf("%v hashMaker counter expected:%v, actual:%v", c.describe, c.hashMaker.ExpectedPosition, c.hashMaker.CurrentPosition)
		}
	}
}


func TestGetMultiPartSize(t *testing.T) {
	cases := []struct {
		describe string
		fileSize int
		partCnt  int
		partSize int
		err      error
	}{
		{
			"パート数が0のときエラー",
			0,
			1,
			0,
			errors.New("invalid part count"),
		},
		{
			"パート数が2以上のファイルサイズのときマルチパートとする",
			55489944,
			11,
			5 * 1024 * 1024,
			nil,
		},
		{
			"パート数が2ピッタリのファイルサイズのときマルチパートとする",
			16 * 1024 * 1024,
			2,
			8 * 1024 * 1024,
			nil,
		},
	}
	for _, c := range cases {
		partSize, err := GetMultiPartSize(c.fileSize, c.partCnt)

		// 戻り値が期待値と一致するか
		if partSize != c.partSize {
			t.Errorf("%v partSize expected:%v, actual:%v", c.describe, c.partSize, partSize)
		}

		// エラーが期待値と一致するか
		if c.err != nil {
			if err != nil {
				if err.Error() != c.err.Error() {
					t.Errorf("%v err expected:%v, actual:%v", c.describe, c.err.Error(), err.Error())
				}
			} else {
				t.Errorf("%v err expected:%v, actual:%v", c.describe, c.err.Error(), nil)
			}
		} else {
			if err != nil {
				t.Errorf("%v err expected:%v, actual:%v", c.describe, nil, err.Error())
			}
		}
	}
}
