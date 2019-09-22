package utils

import (
	"bytes"
	"io/ioutil"

	jsoniter "github.com/json-iterator/go"

	"github.com/houseofcat/turbocookedrabbit/models"
)

const (
	gzipCompressionType = "gzip"
	zstdCompressionType = "zstd"

	aesSymmetricType = "aes"
)

// ConvertJSONFileToConfig opens a file.json and converts to RabbitSeasoning.
func ConvertJSONFileToConfig(fileNamePath string) (*models.RabbitSeasoning, error) {

	byteValue, err := ioutil.ReadFile(fileNamePath)
	if err != nil {
		return nil, err
	}

	config := &models.RabbitSeasoning{}
	var json = jsoniter.ConfigFastest
	err = json.Unmarshal(byteValue, config)

	return config, err
}

// ConvertJSONFileToTopologyConfig opens a file.json and converts to Topology.
func ConvertJSONFileToTopologyConfig(fileNamePath string) (*models.TopologyConfig, error) {

	byteValue, err := ioutil.ReadFile(fileNamePath)
	if err != nil {
		return nil, err
	}

	config := &models.TopologyConfig{}
	var json = jsoniter.ConfigFastest
	err = json.Unmarshal(byteValue, config)

	return config, err
}

// ReadJSONFileToInterface opens a file.json and converts to interface{}.
func ReadJSONFileToInterface(fileNamePath string) (interface{}, error) {

	byteValue, err := ioutil.ReadFile(fileNamePath)
	if err != nil {
		return nil, err
	}

	var data interface{}
	var json = jsoniter.ConfigFastest
	err = json.Unmarshal(byteValue, data)

	return &data, err
}

// CreatePayload creates a JSON marshal and optionally compresses and encrypts the bytes.
func CreatePayload(input interface{}, compression *models.CompressionOptions, encryption *models.EncryptOptions) ([]byte, error) {

	var json = jsoniter.ConfigFastest
	data, err := json.Marshal(&input)
	if err != nil {
		return nil, err
	}

	buffer := &bytes.Buffer{}
	if compression.Enabled {
		err := handleCompression(compression, data, buffer)
		if err != nil {
			return nil, err
		}

		// Update data - data is now compressed
		data = buffer.Bytes()
	}

	if encryption.Enabled {
		err := handleEncryption(encryption, data, buffer)
		if err != nil {
			return nil, err
		}

		// Update data - data is now encrypted
		data = buffer.Bytes()
	}

	return data, nil
}

func handleCompression(compression *models.CompressionOptions, data []byte, buffer *bytes.Buffer) error {

	switch compression.Type {
	case zstdCompressionType:
		return CompressWithZstd(data, buffer)
	case gzipCompressionType:
		fallthrough
	default:
		return CompressWithGzip(data, buffer)
	}
}

func handleEncryption(encryption *models.EncryptOptions, data []byte, buffer *bytes.Buffer) error {

	switch encryption.Type {
	case aesSymmetricType:
		fallthrough
	default:
		data, err := EncryptWithAes(data, encryption.Hashkey, 12)

		if err != nil {
			return err
		}

		*buffer = *bytes.NewBuffer(data)

		return nil
	}
}

// ReadPayload unencrypts and uncompresses payloads
func ReadPayload(buffer *bytes.Buffer, compression *models.CompressionOptions, encryption *models.EncryptOptions) error {

	if encryption.Enabled {
		if err := handleDecryption(encryption, buffer); err != nil {
			return err
		}
	}

	if compression.Enabled {
		if err := handleDecompression(compression, buffer); err != nil {
			return err
		}
	}

	return nil
}

func handleDecompression(compression *models.CompressionOptions, buffer *bytes.Buffer) error {

	switch compression.Type {
	case zstdCompressionType:
		return DecompressWithZstd(buffer)
	case gzipCompressionType:
		fallthrough
	default:
		return DecompressWithGzip(buffer)
	}
}

func handleDecryption(encryption *models.EncryptOptions, buffer *bytes.Buffer) error {

	switch encryption.Type {
	case aesSymmetricType:
		fallthrough
	default:
		data, err := DecryptWithAes(buffer.Bytes(), encryption.Hashkey, 12)

		if err != nil {
			return err
		}

		*buffer = *bytes.NewBuffer(data)

		return nil
	}
}
