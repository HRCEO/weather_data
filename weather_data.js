const http = require('http');   // http 사용 헤더 호출
const express = require('express');
const app = express();

const bodyParser = require('body-parser');   // body를 사용할수 있게 해준다.
const schedule = require('node-schedule'); // node scheduler
const request = require('request'); // let 사용 권장
const convert = require('xml-js'); // xml 처리

const mariadb = require('mariadb'); // MariaDB
const pool = mariadb.createPool({
    host: 'laonfarm.laonpeople.com',
    port: '43306',
    database: 'ceres',
    user: 'laonple',
    password: 'lp_0118!',
    connectionLimit: 50,
    multipleStatements : true
}) //DB 정보


app.use(bodyParser.urlencoded({
    extended: true  //html형식이 아니어도, 사용가능하게 하여 주는것
}));

app.use(bodyParser.json()); // 제이슨 사용 가능 설정  -> body 부분 동작을 위해 필요

http.createServer(app).listen(3030);
console.log('server started');

const weatherUpdateScheudle = schedule.scheduleJob('0 0 * * * *', function(){
    startUpdateWeatherDataLogic();
    console.log('weatherUpdate Complect : ',getFormatDate(new Date()));
});

function startUpdateWeatherDataLogic(){
    let outData = {};
    console.log("getWeatherApiDataInsertCode insert")
    getWeatherApiDataInsertCode()
        .then(async (val) => {

            let weatherData = {};
            let DAY = getFormatDate(new Date());

            //날씨 조회 함수
            for (const item of val) {
                weatherData = await updateWeatherData(item);
                await dbUpdateWeatherData(DAY,weatherData);
            }
            outData.val = ("Update Complete Day : " + DAY);
            outData.code ='null';
            outData.num = '000';
            return (outData);

        }, (err) => {
            outData.val = 'updateWeatherData Error'
            outData.code = err;
            outData.num = '001';
            return(outData);
        });
}

function getDateTime() {
    var date = new Date();

    var hour = date.getHours();
    hour = (hour < 10 ? "0" : "") + hour;

    if(hour < 10) hour = '0' + hour;

    var min  = date.getMinutes();
    min = (min < 10 ? "0" : "") + min;

    if(min < 40) hour -= 1;

    return hour;
}

function getFormatDate(date) {
    var year = date.getFullYear();              //yyyy

    var month = (1 + date.getMonth());          //M
    month = month >= 10 ? month : '0' + month;  //month 두자리로 저장

    var day = date.getDate();                   //d
    day = day >= 10 ? day : '0' + day;          //day 두자리로 저장

    return  year + '' + month + '' + day;       //'-' 추가하여 yyyy-mm-dd 형태 생성 가능
}

function dateAddDel(sDate, nNum, type) {
    var yyyy = parseInt(sDate.substr(0, 4), 10);
    var mm = parseInt(sDate.substr(4, 2), 10);
    var dd = parseInt(sDate.substr(6, 2), 10);
    if (type == "d") {
        d = new Date(yyyy, mm-1, dd + nNum);
    } else if (type == "m") {
        d = new Date(yyyy, mm-1 + nNum, dd);
    } else if (type == "y") {
        d = new Date(yyyy + nNum, mm - 1, dd);
    }
    yyyy = d.getFullYear();
    mm = d.getMonth() + 1; mm = (mm < 10) ? '0' + mm : mm;
    dd = d.getDate(); dd = (dd < 10) ? '0' + dd : dd;
    return '' + yyyy + '' +  mm  + '' + dd;
}

// 기상 특보 및 일간 자료 조회
function getWthrWrnList(url,serviceKey,numOfRows,pageNo,dataType,stnld,fromTmFc,toTmFc) {
    console.log('getWthrWrnList started')
    return new Promise(function(resolve, reject)
    {
        let sumUrl = url

        sumUrl += '?serviceKey=' + serviceKey;
        sumUrl += '&numOfRows='  + numOfRows;
        sumUrl += '&pageNo='	 + pageNo;
        sumUrl += '&dataType='   + dataType;
        sumUrl += '&stnld=' 	 + stnld;
        sumUrl += '&fromTmFc='   + fromTmFc;
        sumUrl += '&toTmFc='     + toTmFc;

        request(sumUrl, function (error, response, body)
        {
            let xmlToJson = convert.xml2json(body, {compact: true, spaces: 4});
            const obj = JSON.parse(xmlToJson);

            if(obj == null || obj === '' || !obj.hasOwnProperty('response') || !obj.response.hasOwnProperty('header') || obj.response.header.resultCode._text !== '00')
            {
                let resErr = {}
                let resultCode 	= ''
                let resultMsg = ''
                const objRes = obj == null ? null : obj.response
                if (objRes == null) {
                    resultCode = ''
                    resultMsg = ''
                }
                if (objRes.hasOwnProperty('header')) {
                    if (objRes.header.hasOwnProperty('resultCode')) {
                        resultCode = objRes.header.resultCode._text;
                        resultMsg = objRes.header.resultMsg._text;
                    } else {
                        resultCode = ''
                        resultMsg = ''
                    }
                } else {
                    resultCode = ''
                    resultMsg = ''
                }

                resErr.resultCode  = resultCode
                resErr.resultMsg   = resultMsg

                reject('No Data '+error);
                return;
            }

            const title = obj.response.body.items.item[0].title._text;

            resolve(title);
        });
    });
}

//동네 예보 초단기 실황 40분
function getUltraSrtNcst(url,serviceKey,numOfRows,pageNo,dataType,base_date,base_time,nx,ny) {
    console.log('getUltraSrtNcst started')
    return new Promise(function(resolve, reject)
    {
        let sumUrl = url

        sumUrl += '?serviceKey=' + serviceKey;
        sumUrl += '&numOfRows='  + numOfRows;
        sumUrl += '&pageNo='	 + pageNo;
        sumUrl += '&datatype='   + dataType;
        sumUrl += '&base_date='  + base_date;
        sumUrl += '&base_time='  + base_time;
        sumUrl += '&nx='     	 + nx;
        sumUrl += '&ny='     	 + ny;

        request(sumUrl, function (error, response, body)
        {
            const xmlToJson = convert.xml2json(body, {compact: true, spaces: 4});
            const obj = JSON.parse(xmlToJson);

            if(obj == null || obj === '' || !obj.hasOwnProperty('response') || !obj.response.hasOwnProperty('header') || obj.response.header.resultCode._text !== '00')
            {

                let resErr = {}
                let resultCode 	= ''
                let resultMsg = ''
                const objRes = obj == null ? null : obj.response
                if (objRes == null) {
                    resultCode = ''
                    resultMsg = ''
                }
                if (objRes.hasOwnProperty('header')) {
                    if (objRes.header.hasOwnProperty('resultCode')) {
                        resultCode = objRes.header.resultCode._text;
                        resultMsg = objRes.header.resultMsg._text;
                    } else {
                        resultCode = ''
                        resultMsg = ''
                    }
                } else {
                    resultCode = ''
                    resultMsg = ''
                }

                resErr.resultCode  = resultCode
                resErr.resultMsg   = resultMsg

                reject('No Data '+error);
                return;
            }

            const count = obj.response.body.totalCount._text;

            let resObjArray = []

            for (let i=0; i < count; i++) {
                if (count === 1) resObjArray.push(obj.response.body.items.item)
                else			 resObjArray.push(obj.response.body.items.item[i])
            }

            let resObj = {}

            resObj.TIM = resObjArray[0].baseTime._text;   // 발표 시각
            resObj.PTY = resObjArray[0].obsrValue._text;  //강수 형태 없음(0), 비(1), 비/눈(2), 눈(3), 소나기(4), 빗방울(5), 빗방울/눈날림(6), 눈날림(7)
            resObj.REH = resObjArray[1].obsrValue._text;  // 습도
            resObj.RN1 = resObjArray[2].obsrValue._text;  // 1시간 강수량
            resObj.T1H = resObjArray[3].obsrValue._text;  // 온도
            resObj.VEC = resObjArray[5].obsrValue._text;  // 풍향
            resObj.WSD = resObjArray[7].obsrValue._text;  // 풍속

            resolve(resObj);
        });
    });
}

//일출, 일몰
function getAreaRiseSetInfo(url,serviceKey,locdate,encodedURL) {
    console.log('getAreaRiseSetInfo started')
    return new Promise(function(resolve, reject)
    {
        //url 조합
        let urlData  = url;
        urlData 	+= '?serviceKey='		 + serviceKey;
        urlData 	+= '&locdate='  		 + locdate;
        urlData 	+= '&location=' 		 + encodedURL;

        request(urlData, function (error, response, body)
        {
            let xmlToJson = convert.xml2json(body, {compact: true, spaces: 4});
            const obj = JSON.parse(xmlToJson);

            if((obj == '') || (obj.response.header.resultCode._text != '00')) {

                var resErr = {};
                var resultCode = obj.response.header.resultCode._text;
                var resultMsg = obj.response.header.resultMsg._text;

                resErr.resultCode = resultCode
                resErr.resultMsg = resultMsg

                reject('No Data ' + error);
                return;
            }

            let resObj = {};

            resObj.sunrise = (obj.response.body.items.item.sunrise._text).trim(); // 일출
            resObj.sunset = (obj.response.body.items.item.sunset._text).trim(); // 일몰

            resolve(resObj);
        });
    });
}

async function updateWeatherData(obj) {

    //동네 예보
    let UltraSrtNcst_url 			= 'http://apis.data.go.kr/1360000/VilageFcstInfoService/';
    let UltraSrtNcst_ServiceKey 	= 'uZNBr2jCBgRlANVRScwTw44Tj6J8a%2B%2BtsT680rWJwBiaxBtMyXUH6dzmCZGruimg44I8827ZxAR6iN6ogK5VRQ%3D%3D';

    //일출 일몰
    let getAreaRiseSetInfo_url 		= 'http://apis.data.go.kr/B090041/openapi/service/RiseSetInfoService/getAreaRiseSetInfo';
    let AreaRiseSetInfo_ServiceKey	= 'prcSbDimx462CEzhxDzSZpgD4R8Es1sQuVgC74zSPIyNHoHoGaB2sd4wf6uIYcUwUu0iQA%2F%2Fw9OCwcb%2BHNqNVw%3D%3D';

    //기상청 특보, 과거 일보 정보 정보
    let newsFlash_url 				= 'http://apis.data.go.kr/1360000/WthrWrnInfoService/';
    let newsFlash_ServiceKey 		= 'uZNBr2jCBgRlANVRScwTw44Tj6J8a%2B%2BtsT680rWJwBiaxBtMyXUH6dzmCZGruimg44I8827ZxAR6iN6ogK5VRQ%3D%3D';

    let UltraSrtNcst = null
    let WthrWrnList = null
    let AreaRiseSetInfoRouter = null

    let date         = getFormatDate(new Date())
    let base_date    = dateAddDel(date, 0 , 'd');   // 기준 날짜 (당일)
    let base_time    = getDateTime() + '00'					   // 매 정각 기준발표
    let fromTmFc     = dateAddDel(date, -6 , 'd'); // 6일 전

    let numOfRows    = '10';								 // 페이지 결과수
    let pageNo       = '1';									 // 페이지 번호
    let dataType     = 'XML';								 // 데이터 형식 xml, json

    let ad_zone_code = obj.administrative_zone_code; // 지점 코드
    let nx    		 = obj.X;					    		 // 예보 지점 X
    let ny    		 = obj.Y;       						 // 예보 지점 Y
    let stnld		 = obj.stnId;							 // 지점 아이디
    let location     = obj.special_point_code;               // 지역 정보
    let encodedURL   = encodeURIComponent(location);         // 지역 정보 변환

    var Wthrurl 		 = newsFlash_url + 'getWthrWrnList' 	 // 특보, 과거 일보 정보
    var WhtrserviceKey   = newsFlash_ServiceKey;				 // 서비스키

    let Ultrurl 		 = UltraSrtNcst_url + 'getUltraSrtNcst'  // 초단기 실황
    let UltrserviceKey   = UltraSrtNcst_ServiceKey;				 // 서비스키

    var Areaurl        = getAreaRiseSetInfo_url;                 // 일출 일목 정보
    var AreaserviceKey = AreaRiseSetInfo_ServiceKey;             // 서비스키

    let updateWeatherData = new Object();

    base_time = "0"+base_time;
    base_time = base_time.slice(-4);

    //동네예보 초단기 실황
    try {
        UltraSrtNcst = await getUltraSrtNcst(Ultrurl,UltrserviceKey,numOfRows,pageNo,dataType,base_date,base_time,nx,ny)
        console.log('UltraSrtNcst OK')
    }catch (e){
        console.log('UltraSrtNcst ERROR', e)
    }

    //기상특보
    try {
        WthrWrnList = await getWthrWrnList(Wthrurl,WhtrserviceKey,numOfRows,pageNo,dataType,stnld,fromTmFc,base_date)
        console.log('WthrWrnList OK')
    } catch (e) {
        console.log('WthrWrnList ERROR', e)
    }

    //일출일몰
    try {
        AreaRiseSetInfoRouter = await getAreaRiseSetInfo(Areaurl,AreaserviceKey, base_date, encodedURL)
        console.log('AreaRiseSetInfoRouter OK')
    } catch (e) {
        console.log('AreaRiseSetInfoRouter ERROR',e)
    }

    updateWeatherData.administrative_zone_code  = ad_zone_code;
    updateWeatherData.UltraSrtNcst 			    = UltraSrtNcst;
    updateWeatherData.WthrWrnList 			    = WthrWrnList;
    updateWeatherData.AreaRiseSetInfoRouter     = AreaRiseSetInfoRouter

    return updateWeatherData;
}

function getWeatherApiDataInsertCode() {
    return new Promise(function(resolve, reject) {
        pool.getConnection().then(conn =>{
            console.log("getWeatherApiDataInsertCode start")
            let sql =
                `
                    SELECT
                        administrative_zone_code,stnId, special_point_code,X,Y
                    FROM 
                         weather_data_call_code 
                    WHERE 
                        administrative_zone_code IN (
                            SELECT DISTINCT 
                                 administrative_zone_code 
                            FROM 
                                 farm_info 
                            WHERE 
                                 administrative_zone_code IS NOT NULL
                            )`;
            console.log("sql : ", sql)
            conn.query(sql).then((rows) => {
                console.log("getWeatherApiDataInsertCode");
                resolve(rows);
            }).catch(err => {
                reject(err);
            });
        }).catch(err => {
            reject(err);
        });
    });
}

function dbUpdateWeatherData(DAY,weatherData) {
    return new Promise(function(resolve, reject) {
        pool.getConnection().then(conn =>{
            console.log("updateWeatherData start")

            let TIM = DAY+weatherData.UltraSrtNcst.TIM;

            let sql =
                `
                UPDATE
                    weather_data_call_code
                SET
                    date            = '${TIM}',
                    sunrise         = '${weatherData.AreaRiseSetInfoRouter.sunrise}',
                    sunset          = '${weatherData.AreaRiseSetInfoRouter.sunset}',
                    temperature     = '${weatherData.UltraSrtNcst.T1H}',
                    humidity        = '${weatherData.UltraSrtNcst.REH}',
                    precipitation   = '${weatherData.UltraSrtNcst.RN1}',
                    weather_status  = '${weatherData.UltraSrtNcst.PTY}',
                    weather_report  = '${weatherData.WthrWrnList}',
                    wind_direction  = '${weatherData.UltraSrtNcst.VEC}',
                    wind_speed      = '${weatherData.UltraSrtNcst.WSD}'
                WHERE
                    administrative_zone_code='${weatherData.administrative_zone_code}'
            `
            console.log("sql : ", sql)
            conn.query(sql).then((rows) => {
                console.log("updateWeatherData OK");
                resolve(rows);
            }).catch(err => {
                reject(err);
            });
        }).catch(err => {
            reject(err);
        });
    });
}
