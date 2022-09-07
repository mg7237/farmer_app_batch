#!/usr/bin/env node
const sql = require('mssql');
require('dotenv').config();
require('./config/db.config');
var logger = require('logger').createLogger('batch.log');
const config = require('./config/db.config');
const axios = require('axios');

require('dotenv').config();
require('axios').default;

setInterval(initializeSchedule, 2000);

setInterval(startSchedule, 2000);

async function initializeSchedule() {
    // console.log('Starting batch.log');
    try {
        const scheduleQuery = `Select * from FarmerBlockSchedule a where Convert(INT, Replace(Convert(VARCHAR(8),ScheduleTime,108),':','')) < Convert(INT, Replace(Convert(VARCHAR(8),GetDate(),108),':',''))  and ID NOT IN (Select ScheduleID from IrrigationScheduleTempTable where ScheduleID = a.ID and cast(ExecutionDate AS Date) = cast(getDate() As Date)) and ScheduleActiveYN = 1 and DeleteYN != 1`;
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
                    let formattedFromDate = format(fromDate);
                    let formattedToDate = format(toDate);
                    if (formattedFromDate != formattedToDate) {
                        executionStatus = 'SKIP';
                        sql.query(`Update FarmerBlockSchedule Set ScheduleActiveYN = 0, DeleteYN = 1 Where ID = ${row.ID}`);
                    }
                }
                const insertSQL = `INSERT INTO IrrigationScheduleTempTable (ScheduleID, ExecutionStartTime, ExecutionStatus, ExecutionDate ) Values (${row.ID}, GetDate(), '${executionStatus}', cast(GetDate() as Date))`;

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
    const selectSQL = `SELECT b.ID TempTableID, * FROM FarmerBlockSchedule a join IrrigationScheduleTempTable b on a.ID = b.ScheduleID  WHERE ExecutionStatus = 'START' AND cast(ExecutionDate as Date) = cast(getDate() as Date)`;
    await sql.connect(config.sqlConfig);
    let result = await sql.query(selectSQL);
    console.dir(result);
    if (result.recordset.length > 0) {
        result.recordset.map(async (row) => {
            let deviceList = await getDeviceList(row.FarmerBlockLinkedIDs);

            if (row.ScheduleIrrigationType == 'V') {
                startStopScheduleVolume(row, deviceList);
            } else {
                startStopScheduleTimer(row, deviceList);
            }
            const updateSQL = `Update IrrigationScheduleTempTable set ExecutionStatus =  'INPROGRESS'  where ID = ${row.TempTableID}`;
            console.log(updateSQL);
            await sql.query(updateSQL);
        });
    }
}

async function startStopScheduleVolume(row, deviceList) {
    const volumeTarget = row.ScheduleIrrigationValue;
    executeDownlink(deviceList, true);

    // let timerObject = timer.periodic(async () => {
    //     let currentWaterflow = await getWaterflowData(deviceList);
    //     if (currentWaterflow > volumeTarget) {
    //         executeDownlink(false);
    //         clearTimeout(timerObject);
    //         const updateSQL = `Update IrrigationScheduleTempTable set ExecutionStatus =  'DONE'  where ID = ${row.TempTableID}`;
    //         console.log(updateSQL);
    //         await sql.query(updateSQL);
    //     }
    // });
}

async function startStopScheduleTimer(row, deviceList) {
    const timerTarget = row.ScheduleIrrigationValue;
    executeDownlink(deviceList, true);
    setTimeout(async () => {
        executeDownlink(deviceList, false);
        const updateSQL = `Update IrrigationScheduleTempTable set ExecutionStatus =  'DONE'  where ID = ${row.TempTableID}`;
        console.log(updateSQL);
        await sql.query(updateSQL);
    }, timerTarget * 60 * 1000);

}


async function executeDownlink(devices, switchON) {
    console.log("switchON", switchON);
    const downlinkURL = process.env.DOWNLINK_URL;
    const downlinkBearer = process.env.BEARER_TOKEN;
    const downlinkPayloadOn = 'AwER';
    const downlinkPayloadOff = 'AwAR';

    let devicesArray = devices.split(',');
    const header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + downlinkBearer
    };

    const instance = axios.create({
        baseURL: downlinkURL,
        timeout: 2500,
        headers: header
    });
    // Strin
    devicesArray.forEach(device => {
        postData = {
            "downlinks": [
                {
                    "frm_payload": switchON ? downlinkPayloadOn : downlinkPayloadOff,
                    "f_port": 1,
                    "priority": "NORMAL"
                }
            ]
        };

        route = '/$deviceID/down/replace';

        instance.post('/eui-a840416c318379da/down/replace').then(function (response) {
            console.log('success');
            if (response.statusCode === 200) {
                console.log('success');
                return true;
            } else {
                console.log('failed');
                return false;
            }
        })
            .catch(err => console.log('fail', err));

    });
}

async function getDeviceList(blocks) {

    try {
        await sql.connect(config.sqlConfig);
        let deviceList = '';
        blockArray = blocks.split(",");
        blockArray.map(async (blockID) => {
            let blockInt = parseInt(blockID);
            blockDevicesSQL = `Select * from FarmerDeviceDetails a join DeviceType b on a.DeviceTypeID = b.ID where FarmerSectionDetailsID = ${blockInt} and FarmerSectionType = 'BO'`;
            let result = await sql.query(blockDevicesSQL);

            if (result.recordset.length > 0) {
                result.recordset.map(row => {
                    if (row.fType == 'FCM' || row.fType == 'FCW') {
                        if (deviceList == '') {
                            deviceList = row.DeviceEUIID;
                        }
                        else {
                            deviceList += ',' + row.DeviceEUIID;
                        }
                    }
                });

            }

            plotDevicesSQL = `Select * from FarmerPlotDetails a join FarmerDeviceDetails b on a.ID = b.FarmerSectionDetailsID where FarmerBlockDetailsID = ${blockInt} and FarmerSectionType = 'PO'`;
            let plotResult = await sql.query(blockDevicesSQL);

            if (plotResult.recordset.length > 0) {
                plotResult.recordset.map(row => {
                    if (row.fType == 'FCM' || row.fType == 'FCW') {
                        if (deviceList == '') {
                            deviceList = row.DeviceEUIID;
                        }
                        else {
                            deviceList += ',' + row.DeviceEUIID;
                        }
                    }
                });
            }


        });

        return deviceList;


    } catch (err) {
        console.log('fail');
        console.dir(err);
        return false;
    }


}

async function getWaterflowData(deviceList) {

    try {
        telematicQuery = `Select Sum(WaterflowTickLiters) * 30 / 7.5 WaterFlow from Telematics where DeviceID IN (${deviceList}) and cast(SensorDataPacketDateTime as Date) = cast(GetDate() as Date)`
        let result = await sql.query(telematicQuery);
        return result.recordset[0].WaterFlow;

    } catch (err) {
        console.log('fail');
        console.dir(err);
        return 0;
    }
}


/////

// function format(tickDateTime) {
//     tickyear = '' + tickDateTime.getFullYear();
//     tickMonth = '' + (tickDateTime.getMonth() + 1);
//     tickDate = '' + tickDateTime.getDate();
//     if (tickMonth.length == 1) tickMonth = '0' + tickMonth;
//     if (tickDate.length == 1) tickDate = '0' + tickDate;
//     return tickyear + tickMonth + tickDate;

// }