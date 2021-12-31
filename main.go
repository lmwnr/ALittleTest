package main

import (
	"MXAntiCheatOffline/utils"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

const path = "/Users/fuzexu/go/src/UVtest/firstProblem/test"

//主程序
func main() {

	//先获取文件指针，通过文件指针创建缓冲区，通过缓冲区读取文件获取byte切片
	files, err := ioutil.ReadDir(path)
	if err != nil {
		panic(err)
	}
	var jsonres utils.Jsonresult
	for a, p := range files {
		if a == 0 {
			continue
		} else {
			file, err := os.Open(path + "/" + p.Name())
			if err != nil {
				panic(err)
			}
			reader, err := gzip.NewReader(file)
			if err != nil {
				panic(err)
			}
			bufReader := bufio.NewReader(reader)
			for {
				//按行读入
				message, _, err := bufReader.ReadLine()
				if err == io.EOF {
					break
				}
				error := json.Unmarshal(message, &jsonres)
				if error != nil {
					utils.I++
					//这个是这个情况导致，有字段，但是：后面没有数据，即 {a：}
				}
				utils.Slice++
				if utils.Slice%5000 == 0 {
					fmt.Println("目前读了5000用户数据！")
				}
				for _, value := range jsonres.Events {
					utils.UserSum[*value.UserID]++
					//统计时间缺失的用户数量
					if jsonres.Loss(value) {
						//统计时间相同的用户数量
						jsonres.SameTime(value)
					}
					//获取loginAt持有数量的分布与loginat四个需求的结果
					if jsonres.LossUserID(value) {
						jsonres.LoginAtUV(value)
						jsonres.LoginAtAnid(value)
						jsonres.LoginAtAvid(value)
						jsonres.LoginAtCid(value)
					}
					//统计x-first-install-time与x-last-update-time时间差值的uv分布
					if value.XLastUpdateTime != nil && value.XFirstInstallTime != nil {
						if *value.XLastUpdateTime != "" && *value.XFirstInstallTime != "" {
							jsonres.UVBuild(value)
						}
					}
				}
			}
			file.Close()
		}

	}
	//每个用户总事件数分布
	//计算单个用户缺失比例，然后统计所有用户缺失比例分布
	utils.TimeLoss()
	//每个用户总事件数分布
	//计算单个用户 四个时间戳相同的比例，然后统计所有用户时间戳相同比例分布
	utils.SameStatic()
	fmt.Print("总数据数:\t")
	fmt.Println(utils.Slice)
	//数据字段缺失总人数
	fmt.Print("字段缺失总人数:\t")
	fmt.Println(utils.I)
	//用户总人数
	fmt.Print("用户总人数:\t") //用户总人数
	fmt.Println(len(utils.UserSum))
	//用户时间戳缺失的总人数
	fmt.Print("用户时间戳缺失的总人数:\t")
	fmt.Println(len(utils.TimeLossUser))
	//四个时间戳相同的总人数
	fmt.Print("四个时间戳相同的总人数:\t")
	fmt.Println(len(utils.SameUser))
	fmt.Printf("四个时间戳任意缺失一个，uv占比%v %", utils.LossUv(len(utils.TimeLossUser), len(utils.UserSum)))
	fmt.Printf("四个时间戳完全相等，uv占比%v %", utils.SameUv(len(utils.SameUser), len(utils.UserSum)))
	fmt.Println()
	//时间戳相差UV分布
	fmt.Println("时间戳相差UV分布:")
	temp := make([]int, 0)
	for key, _ := range utils.UvMap {
		temp = append(temp, int(key))
	}
	sort.Ints(temp)
	for _, value := range temp {
		fmt.Printf("[%v:%v]", value, len(utils.UvMap[int64(value)]))
	}
	fmt.Println()
	fmt.Println("时间戳缺失的总事件分布:")
	fmt.Println(utils.LossUser)
	fmt.Println("时间戳缺失的用户比例分布:")
	fmt.Println(utils.LossPro)
	fmt.Println("时间戳相同的总事件分布:")
	fmt.Println(utils.SameGet)
	fmt.Println("时间戳相同的用户比例分布:")
	fmt.Println(utils.SameSta)
	//id缺失UV占比
	fmt.Printf("三个id任意缺失一个，uv占比:%v %", float64(len(utils.LossId))/float64(len(utils.UserSum))*100)
	for _, l := range utils.LoginAtPro {
		utils.LoginAtUV[len(l)]++
	}
	fmt.Println()
	fmt.Println("loginAt持有数量的uv分布:")
	fmt.Println(utils.LoginAtUV)
	fmt.Printf("loginAt对应的三id组合大于1种，uv占比:%v %", float64(utils.NumPro)/float64(len(utils.UserSum))*100)
	fmt.Println()
	fmt.Printf("loginAt对应cid组合大于1种，uv占比:%v %", float64(utils.NumCid)/float64(len(utils.UserSum))*100)
	fmt.Println()
	fmt.Printf("loginAt对应anid组合大于1种，uv占比:%v %", float64(utils.NumAnid)/float64(len(utils.UserSum))*100)
	fmt.Println()
	fmt.Printf("loginAt对应avid组合大于1种，uv占比:%v %", float64(utils.NumAvid)/float64(len(utils.UserSum))*100)
	//err=setConditions()
	//if err!=nil {
	//	panic(err)
	//}
	//获取黑名单用户的所有数据
	err = utils.UserFind()
	if err != nil {
		panic(err)
	}
	//检查黑名单上的用户
	err = jsonres.WriterBlackUser()
	if err != nil {
		panic(err)
	}

}
