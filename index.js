#!/usr/bin/env node
const sql = require('mssql');
require('dotenv').config();
require('./config/db.config');
var logger = require('logger').createLogger('batch.log');
const config = require('./config/db.config');
const axios = require('axios');
var moment = require('moment-timezone');

require('dotenv').config();
require('axios').default;

setInterval(initializeSchedule, 3000);

setInterval(startSchedule, 4000);

setInterval(stopSchedule, 5000);

async function initializeSchedule() {
    // console.log('Starting batch.log');
    try {
        const indiaTimeZone = 'Asia/Kolkata';
        const timeUTC = new Date();
        var localDate = moment(timeUTC).tz(indiaTimeZone).format(); // .toDate();
        console.log(localDate);
        let executionDate = localDate.substring(0, 10);
        console.log(localDate, executionDate);

        const localTimeInt = parseInt((localDate.substring(11, 13) + localDate.substring(14, 16) + '00'));
        console.log(localTimeInt);
        const scheduleQuery = `Select * from FarmerBlockSchedule a where Convert(INT, Replace(Convert(VARCHAR(8),ScheduleTime,108),':','')) < ${localTimeInt}  and ID NOT IN (Select ScheduleID from IrrigationScheduleTempTable where ScheduleID = a.ID and cast(ExecutionDate AS Date) = '${executionDate}') and ScheduleActiveYN = 1 and DeleteYN != 1`;
        //console.log(scheduleQuery);
        await sql.connect(config.sqlConfig);
        let result = await sql.query(scheduleQuery);
        // console.dir(result);
        //console.dir(result.recordset);

        let executionStatus = '';
        let count = 0;
        let array = result.recordset;
        count = result.recordset.length;
        if (count > 0) {
            //console.log('jhh2', result.recordset.length);
            array.map(async (row) => {
                //console.log("ScheduleType", row["ScheduleType"], row.ScheduleType);
                let blocks = row.FarmerBlockLinkedIDs ?? '';
                //console.di    r("row");
                if (row["ScheduleType"] == 'W') {
                    let dayOfWeek = new Date().getDay();
                    //console.log("dayOfWeek", dayOfWeek);
                    let onOffBit = row.ScheduleWeeks.substring(dayOfWeek, dayOfWeek + 1);
                    // console.log("onOffBit", onOffBit);
                    if (onOffBit == null || onOffBit == '0') {
                        executionStatus = 'SKIP'
                    } else {
                        executionStatus = 'START'
                    }
                } else if (row.ScheduleType == 'A') {
                    let fromDate = new Date(row.ScheduleFromDate);
                    let toDate = new Date();
                    let formattedFromDate = sqlDateFormat(fromDate);
                    let formattedToDate = sqlDateFormat(toDate);
                    if (formattedFromDate == formattedToDate) {
                        executionStatus = 'START';
                        await sql.connect(config.sqlConfig);
                        sql.query(`Update FarmerBlockSchedule Set ScheduleActiveYN = 0, DeleteYN = 1 Where ID = ${row.ID}`);
                    }
                }
                const insertSQL = `INSERT INTO IrrigationScheduleTempTable (ScheduleID, ExecutionStartTime, ExecutionStatus, ExecutionDate, Target, TargetType, FarmerBlockLinkedIDs ) Values (${row.ID}, GetDate(), '${executionStatus}', cast (GetDate() as Date), ${row.ScheduleIrrigationValue}, '${row.ScheduleIrrigationType}', '${row.FarmerBlockLinkedIDs}')`;
                //console.log(insertSQL);
                await sql.connect(config.sqlConfig);
                let insertResult = await sql.query(insertSQL);
                //console.dir(insertResult);
            });
        }

    } catch (e) {
        console.log(e);
        logger.error(e.message);
    }
}

async function startSchedule() {
    const selectSQL = `SELECT ID TempTableID, * FROM IrrigationScheduleTempTable WHERE ExecutionStatus = 'START' AND cast(ExecutionDate as Date) = cast(getDate() as Date)`;
    await sql.connect(config.sqlConfig);
    let result = await sql.query(selectSQL);
    //console.dir(result);
    if (result.recordset.length > 0) {
        result.recordset.map(async (row) => {
            let deviceList = await getDeviceList(row.FarmerBlockLinkedIDs, false);
            console.log("row.FarmerBlockLinkedIDs", row.FarmerBlockLinkedIDs);
            if (deviceList == '') return;
            const resultDownlink = await executeDownlink(deviceList, true);
            let executionStatus = 'ERROR'
            if (resultDownlink) executionStatus = 'INPROGRESS';
            const updateSQL = `Update IrrigationScheduleTempTable set ExecutionStatus =  '${executionStatus}'  where ID = ${row.TempTableID}`;
            //    console.log(updateSQL);
            let updateResult = await sql.query(updateSQL);
        });
    }
}

async function stopSchedule() {
    const selectSQL = `SELECT  * FROM  IrrigationScheduleTempTable WHERE ExecutionStatus = 'INPROGRESS' AND cast(ExecutionDate as Date) = cast(getDate() as Date)`;
    //console.log(selectSQL);
    await sql.connect(config.sqlConfig);
    let result = await sql.query(selectSQL);
    //console.dir(result);
    if (result.recordset.length > 0) {
        for (let i = 0; i < result.recordset.length; i++) {
            console.log("row");
            //console.dir(row);
            let sqlDeviceList = await getDeviceList(result.recordset[i].FarmerBlockLinkedIDs, true);
            let completed = await checkCompletion(result.recordset[i], sqlDeviceList);
            if (completed) {
                let deviceList = await getDeviceList(result.recordset[i].FarmerBlockLinkedIDs, false);
                const resultDownlink = await executeDownlink(deviceList, false);
                console.log('complete downlinkURL', resultDownlink);
                let executionStatus = 'ERROR'
                if (resultDownlink) executionStatus = 'DONE';
                const updateSQL = `Update IrrigationScheduleTempTable set ExecutionStatus = '${executionStatus}'  where ID = ${result.recordset[i].ID}`;
                console.log(updateSQL);
                await sql.query(updateSQL);
            }
        }
    }
}

async function checkCompletion(row, deviceList) {

    let completed = false;
    if (row.target != null && row.TargetType == 'V') {
        console.log('VVV');
        let currentWaterflow = await getWaterflowData(deviceList);
        if (currentWaterflow > row.target) {
            completed = true;
        }
    }
    else {
        console.log('TTTT');
        let diff = (new Date() - row.ExecutionStartTime) / (1000 * 60);
        console.log('Diff: ' + diff);
        if (diff > row.target) {
            completed = true;
        }
    }
    return completed;
}

async function executeDownlink(deviceList, switchON) {
    console.log("switchON", switchON);
    var downlinkURL = process.env.DOWNLINK_URL;
    const downlinkBearer1 = process.env.BEARER_TOKEN1;
    const downlinkBearer2 = process.env.BEARER_TOKEN2;
    const downlinkPayloadOn = 'AwER';
    const downlinkPayloadOff = 'AwAR';


    let devices = '';
    devices = deviceList;
    let devicesArray = devices.toString().split(',');
    console.log('deviceList: ' + deviceList);
    console.dir(devicesArray);
    let header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + downlinkBearer1
    };


    // String
    try {
        if (devicesArray.length > 0) {
            for (let i = 0; i < devicesArray.length; i++) {
                downlinkURL = `https://cultyvate.eu1.cloud.thethings.industries/api/v3/as/applications/99808/devices/eui-a840416c318379da/down/replace`;

                postData = {
                    "downlinks": [
                        {
                            "frm_payload": switchON ? downlinkPayloadOn : downlinkPayloadOff,
                            "f_port": 1,
                            "priority": "NORMAL"
                        }
                    ]
                };

                if (devicesArray[i] == "a84041182182460a") {
                    downlinkURL = `https://cultyvate.eu1.cloud.thethings.industries/api/v3/as/applications/ib-testing-v3/devices/a84041182182460a/down/replace`;

                    header = {
                        "Content-Type": "application/json",
                        "Authorization": "Bearer " + downlinkBearer2
                    };

                    postData = {
                        "downlinks": [
                            {
                                "frm_payload": switchON ? 'AAI' : 'AAE',
                                "f_port": 1,
                                "priority": "NORMAL"
                            }
                        ]
                    };

                }

                let instance = await axios.create({
                    timeout: 2500,
                    headers: header
                });

                let response = await instance.post(downlinkURL, postData);
                console.log('success');
                if (response.status === 200) {
                    console.log('success', response.statusCode);
                } else {
                    console.log('failed');
                    console.dir(response);
                    return false;
                }
            }
            return true;

        } else {
            return false;
        }
    } catch (err) {
        console.log('fail service', err);
        return false;
    }
}

async function getDeviceList(blocks, sqlDevices) {
    try {
        console.log('ENTER', blocks);
        await sql.connect(config.sqlConfig);
        let deviceList = '';
        let sqlDeviceList = '';
        let blockArray = blocks.split(",");
        console.dir(blockArray);

        for (let i = 0; i < blockArray.length; i++) {
            blockDevicesSQL = `Select * from FarmerDeviceDetails a join DeviceType b on a.DeviceTypeID = b.ID where FarmerSectionDetailsID = '${blockArray[i]}' and FarmerSectionType = 'BO'`;
            console.log(blockDevicesSQL);
            let result = await sql.query(blockDevicesSQL);

            if (result.recordset.length > 0) {
                result.recordset.map(row => {
                    if (row.fType == 'FCM' || row.fType == 'FCW') {
                        console.log('Block Device: ' + row.fType + row.DeviceEUIID);
                        if (deviceList == '') {
                            deviceList = row.DeviceEUIID;
                            sqlDeviceList = `'` + row.DeviceEUIID + `'`;
                        }
                        else {
                            deviceList += ',' + row.DeviceEUIID;
                            sqlDeviceList += `,'` + row.DeviceEUIID + `'`;
                        }
                    }
                });

            }
        }


        console.dir(blockArray);

        for (var i = 0; i < blockArray.length; i++) {

            plotDevicesSQL = `Select * from FarmerPlotsDetails a join FarmerDeviceDetails b on a.ID = b.FarmerSectionDetailsID join DeviceType c on b.DeviceTypeID = c.ID where FarmerBlockDetailsID = '${blockArray[i]}' and FarmerSectionType = 'PO'`;
            console.log(plotDevicesSQL);
            let plotResult = await sql.query(plotDevicesSQL);
            if (plotResult.recordset.length > 0) {
                for (let j = 0; j < plotResult.recordset.length; j++) {
                    console.log('PLOT Device: ' + plotResult.recordset[j].fType + plotResult.recordset[j].DeviceEUIID);
                    if (plotResult.recordset[j].fType == 'FCM' || plotResult.recordset[j].fType == 'FCW') {
                        console.log('FCM Device: ' + plotResult.recordset[j].fType + plotResult.recordset[j].DeviceEUIID);

                        if (deviceList == '') {
                            deviceList = plotResult.recordset[j].DeviceEUIID;
                            sqlDeviceList = `'` + plotResult.recordset[j].DeviceEUIID + `'`;
                        }
                        else {
                            deviceList += ',' + plotResult.recordset[j].DeviceEUIID;
                            sqlDeviceList += `,'` + plotResult.recordset[j].DeviceEUIID + `'`;
                        }
                    }
                }
            }

        }

        console.log("deviceList:::::", deviceList);
        if (sqlDevices) {
            return sqlDeviceList;
        } else {
            return deviceList;
        }
    } catch (err) {
        console.log('fail list');
        console.dir(err);
        return '';
    }
}


async function getWaterflowData(deviceList) {
    try {
        telematicQuery = `Select Sum(WaterflowTickLiters) * 30 / 7.5 WaterFlow from Telematics where DeviceID IN (${deviceList}) and cast(SensorDataPacketDateTime as Date) = cast(GetDate() as Date)`
        await sql.connect(config.sqlConfig);
        let result = await sql.query(telematicQuery);
        return result.recordset[0].WaterFlow;

    } catch (err) {
        console.log('fail');
        console.dir(err);
        return 0;
    }
}

function sqlDateFormat(tickDateTime) {
    tickyear = '' + tickDateTime.getFullYear();
    tickMonth = '' + (tickDateTime.getMonth() + 1);
    tickDate = '' + tickDateTime.getDate();
    if (tickMonth.length == 1) tickMonth = '0' + tickMonth;
    if (tickDate.length == 1) tickDate = '0' + tickDate;
    return tickyear + tickMonth + tickDate;

}