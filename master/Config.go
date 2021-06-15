package master

import (
	"encoding/json"
	"io/ioutil"
)

// Config 程序配置
type Config struct {
	ApiPort               int      `json:"apiPort"`
	ApiReadTimeout        int      `json:"apiReadTimeout"`
	ApiWriteTimeout       int      `json:"apiWriteTimeout"`
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	WebRoot               string   `json:"webroot"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
}

var (
	// GConfig 单例
	GConfig *Config
)

// InitConfig 加载配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	//1,把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	//2,做Json反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	//3,赋值单例
	GConfig = &conf
	return
}
