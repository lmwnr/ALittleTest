package utils

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
)

const (
	path         = "/Users/fuzexu/go/src/UVtest/firstProblem/test"
	normalPath   = "/Users/fuzexu/go/src/UVtest/firstProblem/normal.text"
	abnormalPath = "/Users/fuzexu/go/src/UVtest/firstProblem/abnormal.text"
	blackPath    = "/Users/fuzexu/go/src/UVtest/firstProblem/blacklist.text"
	sedConPath   = "/Users/fuzexu/go/src/UVtest/firstProblem/00-03/111"
	wbPath       = "/Users/fuzexu/go/src/UVtest/firstProblem/result/"
)

var (
	//记录读取数据时错误的总数
	I int
	//获取用户总数同时记录每个用户有多少条数据
	UserSum = make(map[string]int)
	//时间戳相同的用户统计结果的去重
	SameUser = make(map[string]int)
	//时间戳缺失的用户统计结果的去重
	TimeLossUser = make(map[string]int)
	//x-first-install-time与x-last-update-time的时间差值UV分布
	UvMap = make(map[int64]map[string]int)
	//总数据数
	Slice int
	//时间戳缺失的用户事件数分布
	LossUser = make(map[int]int)
	//时间戳缺失的用户比例分布
	LossPro = make(map[float64]int)
	//时间戳相同的总事件分布
	SameGet = make(map[int]int)
	//时间戳相同的用户比例分布
	SameSta = make(map[float64]int)
	//记录任意一个id缺失
	LossId = make(map[string]int)
	//同一个loginAt三个ID组合数大于一的用户数
	NumPro int
	//同一个loginAt中Cid数量大于一的用户数
	NumCid int
	//同一个loginAt中Avid数量大于一的用户数
	NumAvid int
	//同一个loginAt中Anid数量大于一的用户数
	NumAnid int
	//loginAt数量的分布
	LoginAtUV = make(map[int]int)
	//组合数的存储
	LoginAtPro  = make(map[string]map[int][]string)
	LoginSum    = make(map[string]map[int]int)
	JudgeProAll = make(map[string]bool)
	//Cid的存储
	Cid      = make(map[string]map[int]string)
	JudgeCid = make(map[string]bool)
	//Avid的存储
	Avid      = make(map[string]map[int]string)
	JudgeAvid = make(map[string]bool)
	//Anid的存储
	Anid        = make(map[string]map[int]string)
	JudgeAnid   = make(map[string]bool)
	LossPercent = make(map[string]float64)
	JudgePro    = true
	//存储黑名单用户
	BlackUser = make(map[string]int)
)

//解码JSON所用的结构体
type Jsonresult struct {
	Events []Events `json:"events"`
}
type Events struct {
	Ts                *int64  `json:"ts"`
	Event             *string `json:"event"`
	UserID            *string `json:"userId"`
	LoginAt           *int    `json:"loginAt"`
	API               *string `json:"api"`
	XForwardedFor     *string `json:"x-forwarded-for"`
	XClientID         *string `json:"x-client-id"`
	XAvID             *string `json:"x-av-id"`
	XAnID             *string `json:"x-an-id"`
	XMxTimestamp      *string `json:"x-mx-timestamp"`
	XTimestamp        *string `json:"x-timestamp"`
	XLastUpdateTime   *string `json:"x-last-update-time"`
	XFirstInstallTime *string `json:"x-first-install-time"`
}

//编码所用结构体
type JsonWriter struct {
	UserID  string        `json:"userid"`
	Actions []interface{} `json:"actions"`
}
type Actions struct {
	Action int64 `json:"action"`
	Total  int64 `json:"total"`
	Hit    int64 `json:"hit"`
}
type ActionLogin struct {
	Action  int64 `json:"action"`
	LoginAt int64 ` json:"loginat"`
	Total   int64 `json:"total"`
	Hit     int64 `json:"hit"`
}

//黑名单编码所用结构体
type BlackName struct {
	UserID string   `json:"userid"`
	Event  []string `json:"event"'`
}

//获取黑名单用户的所有数据
func (jsonres *Jsonresult) WriterBlackUser() error {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	for a, p := range files {
		if a == 0 {
			continue
		} else {
			file, err := os.Open(path + "/" + p.Name())
			if err != nil {
				return err
			}
			reader, err := gzip.NewReader(file)
			if err != nil {
				return err
			}
			bufReader := bufio.NewReader(reader)
			for {
				//按行读入
				message, _, err2 := bufReader.ReadLine()
				if err2 == io.EOF {
					break
				}
				json.Unmarshal(message, &jsonres)
				for _, value := range jsonres.Events {
					if BlackUser[*value.UserID] != 0 {
						abFile, err := os.OpenFile(wbPath+*value.UserID, os.O_APPEND|os.O_RDWR, 0664)
						if err != nil {
							abFile, _ = os.Create(wbPath + *value.UserID)
						}
						w := bufio.NewWriter(abFile)
						data, err := json.Marshal(value)
						if err != nil {
							return err
						}
						data = append(data, '\n')
						_, err = w.Write(data)
						if err != nil {
							return err
						}
						w.Flush()
						abFile.Close()
					}
				}
			}
			file.Close()
		}
	}
	return nil
}

//检查黑名单上的用户
func UserFind() error {
	filenormal, err := os.OpenFile(normalPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	wnor := bufio.NewWriter(filenormal)
	defer filenormal.Close()
	fileabnormal, err := os.OpenFile(abnormalPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	wabnor := bufio.NewWriter(fileabnormal)
	defer fileabnormal.Close()
	file, err := os.Open(blackPath)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if UserSum[line] != 0 {
			bn := BlackName{
				UserID: line,
			}
			events := make([]string, 0)
			if TimeLossUser[line] != 0 {
				event := "时间戳缺失!"
				events = append(events, event)
			}
			if SameUser[line] != 0 {
				event := "时间戳全部相同!"
				events = append(events, event)

			}
			if len(LoginAtPro[line]) > 1 {
				event := "LoginAt数量大于1!"
				events = append(events, event)
			}
			for _, value := range LoginAtPro[line] {
				if len(value) > 1 {
					event := "同一个LoginAt对应的组合数大于1!"
					events = append(events, event)
					break
				}
			}
			if len(events) == 0 {
				var build strings.Builder
				build.WriteString(line)
				build.WriteString("\n")
				_, err = wnor.WriteString(build.String())
				if err != nil {
					return err
				}
			} else {
				bn.Event = events
				data, err := json.Marshal(bn)
				if err != nil {
					return err
				}
				data = append(data, '\n')
				_, err = wabnor.Write(data)
				if err != nil {
					return err
				}
				//方便第二次遍历时获取所有的黑名单上用户的数据
				BlackUser[line] = 1
			}
		}
	}
	wnor.Flush()
	wabnor.Flush()
	return nil
}

//设置条件
func (writer *JsonWriter) SetConditions() error {
	f, err := os.OpenFile(sedConPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for key, value := range UserSum {
		jw := JsonWriter{
			UserID: key,
		}
		actions := make([]interface{}, 0)
		if TimeLossUser[key] != 0 {
			if LossPercent[key] >= 0.5 {
				action := Actions{
					Action: 1,
					Total:  int64(value),
					Hit:    int64(TimeLossUser[key]),
				}
				actions = append(actions, action)
			}
		}

		if SameUser[key] != 0 {
			action := Actions{
				Action: 2,
				Total:  int64(value),
				Hit:    int64(SameUser[key]),
			}
			actions = append(actions, action)
		}
		if len(LoginAtPro[key]) != 0 {
			for s, m := range LoginAtPro[key] {
				if len(m) > 1 {
					action := ActionLogin{
						Action:  3,
						LoginAt: int64(s),
						Total:   int64(LoginSum[key][s]),
						Hit:     int64(len(m)),
					}
					actions = append(actions, action)
				}
			}
		}
		jw.Actions = actions
		if len(jw.Actions) > 0 {
			err = jw.WriterFile(w)
			if err != nil {
				return err
			}
			w.Flush()
		}
	}
	return nil
}

//写入数据
func (writer *JsonWriter) WriterFile(w *bufio.Writer) error {
	data, err := json.Marshal(writer)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	return nil
}

//loginat对应的三个ID组合大于1的UV占比
func (jsonres *Jsonresult) LoginAtUV(event Events) {
	//先判断该用户是否是第一次赋值
	usersPro, err := LoginAtPro[*event.UserID]
	logSum := LoginSum[*event.UserID]
	if err {
		//先判断该LoginAt是不是与之前的不同
		userPro, err := usersPro[*event.LoginAt]
		//true代表是同一个用户的同一个loginat需要判断组合数，但不需要存储
		if err {
			logSum[*event.LoginAt]++
			//如果是false代表该用户目前的组合数不大于1，没有计入总数
			//如果是true代表该用户组合数已经大于1，已经计入总数，可以跳过
			var build strings.Builder
			build.WriteString(*event.XClientID)
			build.WriteString(*event.XAvID)
			build.WriteString(*event.XAnID)
			for _, value := range userPro {
				if strings.Compare(value, build.String()) == 0 {
					//拼接后的字符串相同，表示组合数一致，直接跳过
					JudgePro = false
				}
			}
			if JudgePro {
				if len(userPro) == 1 && JudgeProAll[*event.UserID] == false {
					NumPro++
					JudgeProAll[*event.UserID] = true
				}

				userPro = append(userPro, build.String())
				usersPro[*event.LoginAt] = userPro
				LoginAtPro[*event.UserID] = usersPro
			}
			JudgePro = true

			//false代表是同一个用户不同的login，需要存储到map中
		} else {
			logSum[*event.LoginAt]++
			var build strings.Builder
			build.WriteString(*event.XClientID)
			build.WriteString(*event.XAvID)
			build.WriteString(*event.XAnID)
			userPro = append(userPro, build.String())
			usersPro[*event.LoginAt] = userPro
			LoginAtPro[*event.UserID] = usersPro
		}
	} else {
		usersPro = make(map[int][]string)
		logSum = make(map[int]int)
		var build strings.Builder
		build.WriteString(*event.XClientID)
		build.WriteString(*event.XAvID)
		build.WriteString(*event.XAnID)
		usersPro[*event.LoginAt] = append(usersPro[*event.LoginAt], build.String())
		LoginAtPro[*event.UserID] = usersPro
		logSum[*event.LoginAt]++
		LoginSum[*event.UserID] = logSum
	}
}

//loginat对应的三个ID中Cid大于1的UV占比
func (jsonres *Jsonresult) LoginAtCid(event Events) {
	//先判断该用户是否是第一次赋值
	usersCid, err := Cid[*event.UserID]
	if err {
		userCid, err := usersCid[*event.LoginAt]
		//true代表是同一个用户的同一个loginat需要判断组合数，但不需要存储
		if err {
			//如果是false代表该用户目前的组合数不大于1，没有计入总数
			//如果是true代表该用户组合数已经大于1，已经计入总数，可以跳过
			if JudgeCid[*event.UserID] {
			} else {
				if strings.Compare(userCid, *event.XClientID) == 0 {
					//拼接后的字符串相同，表示组合数一致，直接跳过
				} else {
					//拼接后的字符串不同，代表组合数大于一，计入总数，并修改标记
					NumCid++
					JudgeCid[*event.UserID] = true
				}
			}
			//false代表是同一个用户不同的login，需要存储到map中
		} else {
			var build strings.Builder
			build.WriteString(*event.XClientID)
			build.WriteString(*event.XAvID)
			build.WriteString(*event.XAnID)
			userCid = *event.XClientID
			usersCid[*event.LoginAt] = userCid
			Cid[*event.UserID] = usersCid
		}
	} else {
		usersCid = make(map[int]string)
		var build strings.Builder
		build.WriteString(*event.XClientID)
		build.WriteString(*event.XAvID)
		build.WriteString(*event.XAnID)
		usersCid[*event.LoginAt] = *event.XClientID
		Cid[*event.UserID] = usersCid
	}
}

//loginat对应的三个ID中Avid大于1的UV占比
func (jsonres *Jsonresult) LoginAtAvid(event Events) {
	//先判断该用户是否是第一次赋值
	usersAvid, err := Avid[*event.UserID]
	if err {
		//先判断该LoginAt是不是与之前的不同
		userAvid, err := usersAvid[*event.LoginAt]
		//true代表是同一个用户的同一个loginat需要判断组合数，但不需要存储
		if err {
			//如果是false代表该用户目前的组合数不大于1，没有计入总数
			//如果是true代表该用户组合数已经大于1，已经计入总数，可以跳过
			if JudgeAvid[*event.UserID] {
			} else {
				if strings.Compare(userAvid, *event.XAvID) == 0 {
					//拼接后的字符串相同，表示组合数一致，直接跳过
				} else {
					//拼接后的字符串不同，代表组合数大于一，计入总数，并修改标记
					NumAvid++
					JudgeAvid[*event.UserID] = true
				}
			}
			//false代表是同一个用户不同的login，需要存储到map中
		} else {
			var build strings.Builder
			build.WriteString(*event.XClientID)
			build.WriteString(*event.XAvID)
			build.WriteString(*event.XAnID)
			userAvid = *event.XAvID
			usersAvid[*event.LoginAt] = userAvid
			Avid[*event.UserID] = usersAvid
		}
	} else {
		usersAvid = make(map[int]string)
		var build strings.Builder
		build.WriteString(*event.XClientID)
		build.WriteString(*event.XAvID)
		build.WriteString(*event.XAnID)
		usersAvid[*event.LoginAt] = *event.XAvID
		Avid[*event.UserID] = usersAvid
	}
}

//loginat对应的三个ID中Anid大于1的UV占比
func (jsonres *Jsonresult) LoginAtAnid(event Events) {
	//先判断该用户是否是第一次赋值
	usersAnid, err := Anid[*event.UserID]
	if err {
		//先判断该LoginAt是不是与之前的不同
		userAnid, err := usersAnid[*event.LoginAt]
		//true代表是同一个用户的同一个loginat需要判断组合数，但不需要存储
		if err {
			//如果是false代表该用户目前的组合数不大于1，没有计入总数
			//如果是true代表该用户组合数已经大于1，已经计入总数，可以跳过
			if JudgeAnid[*event.UserID] {
			} else {
				if strings.Compare(userAnid, *event.XAnID) == 0 {
					//拼接后的字符串相同，表示组合数一致，直接跳过
				} else {
					//拼接后的字符串不同，代表组合数大于一，计入总数，并修改标记
					NumAnid++
					JudgeAnid[*event.UserID] = true
				}
			}
			//false代表是同一个用户不同的login，需要存储到map中
		} else {
			var build strings.Builder
			build.WriteString(*event.XClientID)
			build.WriteString(*event.XAvID)
			build.WriteString(*event.XAnID)
			userAnid = *event.XAnID
			usersAnid[*event.LoginAt] = userAnid
			Anid[*event.UserID] = usersAnid
		}
	} else {
		usersAnid = make(map[int]string)
		var build strings.Builder
		build.WriteString(*event.XClientID)
		build.WriteString(*event.XAvID)
		build.WriteString(*event.XAnID)
		usersAnid[*event.LoginAt] = *event.XAnID
		Anid[*event.UserID] = usersAnid
	}
}

//时间戳缺失的用户事件数分布
func (jsonres *Jsonresult) LossUserID(event Events) bool {
	if event.XAnID == nil || event.XAvID == nil || event.XClientID == nil || *event.XAnID == "" || *event.XAvID == "" || *event.XClientID == "" {
		LossId[*event.UserID]++
		return false
	} else {
		return true
	}
}

//每个用户总事件数分布
//计算单个用户 四个时间戳相同的比例，然后统计所有用户时间戳相同比例分布
func SameStatic() {
	for key, value := range SameUser {
		//存储用户总事件分布
		SameGet[UserSum[key]]++
		//计算单个用户的缺失比例，向小数点后一位取整，精确到0.1
		sameProportion := float64(value) / float64(UserSum[key])
		integer, decimal := math.Modf(sameProportion * 10)
		if decimal == 0 {
			sameProportion = integer / 10
		} else {
			sameProportion = (integer + 1) / 10
		}
		SameSta[sameProportion]++
	}
}

//每个用户总事件数分布
//计算单个用户缺失比例，然后统计所有用户缺失比例分布
func TimeLoss() {
	for key, value := range TimeLossUser {
		//存储用户总事件分布
		LossUser[UserSum[key]]++
		//计算单个用户的缺失比例，向小数点后一位取整，精确到0.1
		lossProportion := float64(value) / float64(UserSum[key])
		integer, decimal := math.Modf(lossProportion * 10)
		if decimal == 0 {
			lossProportion = integer / 10
		} else {
			lossProportion = (integer + 1) / 10
		}
		LossPro[lossProportion]++
		LossPercent[key] = lossProportion
	}
}

//四个时间戳任意缺失一个，uv占比
func LossUv(timeLoss int, user int) (res float64) {
	res = float64(timeLoss) / float64(user) * 100
	return res
}

//四个时间戳完全相等，uv占比
func SameUv(same int, user int) (res float64) {
	res = float64(same) / float64(user) * 100
	return res
}

//统计x-first-install-time与x-last-update-time时间差值的uv分布
func (jsonres *Jsonresult) UVBuild(event Events) {
	xfit, _ := strconv.ParseInt(*event.XFirstInstallTime, 10, 64)
	xlut, _ := strconv.ParseInt(*event.XLastUpdateTime, 10, 64)
	sub := xfit - xlut
	year := sub / 31536000000
	if sub == 0 {
		year = 0
	} else {
		if sub > 0 {
			if sub%31536000000 != 0 {
				year++
			}
		} else {
			if sub%31536000000 != 0 {
				year--
			}
		}

	}
	//先判断是否是第一次给map的指定位置赋值，是的话在第一次赋值时必须创建map赋值，不是就直接赋值就行
	users, err := UvMap[year]
	if err {
		user := users[*event.UserID]
		user++
		users[*event.UserID] = user
		UvMap[year] = users
	} else {
		users = make(map[string]int)
		users[*event.UserID]++
		UvMap[year] = users

	}
}

//获取四个时间戳都相等的用户量
func (jsonres *Jsonresult) SameTime(event Events) {
	if *event.XMxTimestamp == *event.XTimestamp && *event.XMxTimestamp == *event.XFirstInstallTime && *event.XMxTimestamp == *event.XLastUpdateTime {
		SameUser[*event.UserID]++
	}
}

//获取四个时间戳任意缺失一个的总数
func (jsonres *Jsonresult) Loss(event Events) bool {
	if event.XMxTimestamp == nil || event.XTimestamp == nil || event.XLastUpdateTime == nil || event.XFirstInstallTime == nil || *event.XMxTimestamp == "" || *event.XTimestamp == "" || *event.XLastUpdateTime == "" || *event.XFirstInstallTime == "" {
		TimeLossUser[*event.UserID]++
		return false
	} else {
		return true
	}
}
