package com.migrate

import java.sql.Date
import java.sql.Timestamp

class CommonUtils {

    static void callTiming(Long t1, String message = "") {
        Long t2 = System.currentTimeMillis()
        println "${message}"
        println "${(t2 - t1)} milliseconds or ${((t2 - t1) / 1000) as Double} seconds or ${((t2 - t1) / 60000) as Double} minutes"
    }

    static void callTiming(Closure closure, String message = "") {
        Long t1 = System.currentTimeMillis()
        closure.call()
        callTiming(t1, message)
    }

    static String convertTimeStamptoDate(Timestamp timestamp) {
        new Date(timestamp.time).format("yyyy-MM-dd")
    }

    static String convertTimeStamptoDate(String timestamp) {
        Date.parse("yyyy-MM-dd hh:mm:ss", timestamp).format("yyyy-MM-dd")
    }

    static String convertTimeStamptoDate(Date date) {
        date.format("yyyy-MM-dd HH:mm")
    }

    static java.util.Date convertDateStringtoDate(String timestamp) {
        Date.parse("yyyy-MM-dd hh:mm:ss", timestamp)
    }

    static Map getMapForAttribute() {
        def firstName = { String value -> ["firstName": value] }
        def lastName = { String value -> ["lastName": value] }
        def middleName = { String value -> ["middleName": value] }
        def mobileNumber = { String value -> ["mobileNumber": value] }
        def description = { String value -> ["description": value] }
        def name = { String value -> ["name": value] }
        def isActive = { String value -> ["isActive": (value == "1")] }
        def street = { String value ->
            List address = value.tokenize("\n")
            Map response = ["addressLine1": address[0]]
            if (address.size() > 1) {
                address.remove(0)
                response["addressLine2"] = address.join("\n")
            }
            response
        }
        def title = { String value ->
            if (value != null) {
                return ["title": value]
            } else {
                return [:]
            }
        }
        [
                "4"  : title,
                "5"  : firstName,
                "6"  : middleName,
                "7"  : lastName,
                "11" : { String value -> ["dateOfBirth": convertTimeStamptoDate(value)] },
                "12" : { String value -> ["password": value] },
                "18" : { String value -> ["gender": CommonService.customerGenderMap[value]] },
                "19" : title,
                "20" : firstName,
                "21" : middleName,
                "22" : lastName,
                "25" : street,
                "26" : { String value -> ["city": value] },
                "28" : { String value -> ["state": value] },
                "30" : { String value -> ["pinCode": value] },
                "31" : mobileNumber,
                "41" : name,
                "42" : isActive,
                "43" : { String value -> ["webURL": value] },
                "44" : description,
                "46" : { String value -> ["pageTitle": value] },
                "47" : { String value -> ["metaKeywords": value] },
                "48" : description,
                "57" : { String value -> ["webUrlPath": value] },
                "66" : { String value -> ["defaultSortOrder": value == "position" ? "BEST_VALUE_INDEX" : "ARRIVAL"] },
                "67" : { String value -> ["isVisible": (value == "1")] },
                "71" : name,
                "72" : description,
                "73" : { String value -> ["shortDescription": value] },
                "75" : { String value -> ["price": value as Float] },
                "80" : { String value -> ["weight": value as Float] },
                "96" : isActive,
                "97" : { String value -> ["urlKey": value] },
                "98" : { String value -> ["urlPath": value] },
                "176": mobileNumber,
                "179": { String value -> ["asStylingTips": value] },
                "180": { String value -> ["compositionCode": value] },
                "181": { String value -> ["coverage": value] },
                "183": { String value -> ["designCode": value] },
                "184": { String value -> ["eanNumber": value] },
                "185": { String value -> [["infoAndCare": value], ["fabric": value]] },
                "189": { String value -> ["styleCode": value] },
                "190": { String value -> ["yearOfManufacture": value] },
                "202": { String value -> ["premiumPackagingSKU": value] },
                "208": { String value -> ["heading1": value] },
                "209": { String value -> ["heading2": value] },
                "216": { String value -> ["department": value] },
                "244": { String value -> ["liveDate": convertDateStringtoDate(value)?.time] },

        ].withDefault {
            { String value -> [:] }
        }
    }

    static List getInputForSuggestions(String input) {
        List inputList = new ArrayList<String>()
        String[] inputStringArr = input.split(" ")
        for (int i = 0; i < inputStringArr.length; i++) {
            for (int j = i; j < inputStringArr.length; j++) {
                if (i == j)
                    inputList.add(inputStringArr[i])
                else
                    inputList.add(inputList.get(inputList.size() - 1) + " " + inputStringArr[j])
            }
        }
        inputList
    }

    static Map createSuggestions(String input, String output, Object payload, Integer weight, List additionalInputs = []) {
        List inputs = getInputForSuggestions(input)
        if (additionalInputs)
            inputs.addAll additionalInputs
        [
                input  : inputs,
                output : output,
                payload: payload,
                weight : weight
        ]
    }

    static List getPathList(String input) {
        List arr = input.tokenize("/").reverse()
        List finalList = []
        finalList << arr.join("/")
        while (arr = arr.tail()) {
            finalList << arr.join("/")
        }
        finalList
    }
}
