
## Vehicle Data Schema
<table>
    <tr>
        <td>Field Name </td>
        <td>Data Type </td>
        <td>Field Size for display </td>
        <td>Description </td>
        <td>Example </td>
    </tr>
    <tr>
        <td>CRASH_UNIT_ID </td>
        <td>Integer </td>
        <td>10 </td>
        <td>A unique identifier for each vehicle record. </td>
        <td>1414411 </td>
    </tr>
    <tr>
        <td>CRASH_ID </td>
        <td>Text </td>
        <td>128 </td>
        <td>This number can be used to link to the same crash in the Crashes and People datasets. This number also serves as a unique ID in the Crashes dataset. </td>
        <td>38328abee0afd4af28 19a18fd1bd4cddf1187160403c3d8 6c20895c7b33ec20627 0ced764c46e64e10 1608b77fadb019ce a75681cde0a4b7ab07b43ff1ebc4c6 </td>
    </tr>
    <tr>
        <td>CRASH_DATE </td>
        <td>Date/Time </td>
        <td>22 </td>
        <td>Date and time of crash as entered by the reporting officer </td>
        <td>9/6/22 23:55</td>
    </tr>
    <tr>
        <td>VEHICLE_ID </td>
        <td>Integer </td>
        <td>10 </td>
        <td>Unique id for the vehicle </td>
        <td>1344101 </td>
    </tr>
    <tr>
        <td>VEHICLE_MAKE </td>
        <td>Text </td>
        <td>15 </td>
        <td>The make (brand) of the vehicle, if relevant </td>
        <td>Audi </td>
    </tr>
    <tr>
        <td>VEHICLE_MODEL </td>
        <td>Text </td>
        <td>15 </td>
        <td>The model of the vehicle, if relevant </td>
        <td>Q5 </td>
    </tr>
    <tr>
        <td>VEHICLE_YEAR </td>
        <td>Integer </td>
        <td>4 </td>
        <td>The model year of the vehicle, if relevant </td>
        <td>2012 </td>
    </tr>
    <tr>
        <td>VEHICLE_TYPE </td>
        <td>Text </td>
        <td>20 </td>
        <td>The type of vehicle, if relevant </td>
        <td>SUV</td>
    </tr>
</table>

## Person Data Schema
<table>
    <tr>
        <td>Field Name </td>
        <td>Data Type </td>
        <td>Field Size for display </td>
        <td>Description </td>
        <td>Example </td>
    </tr>
    <tr>
        <td>PERSON_ID </td>
        <td>Text </td>
        <td>10 </td>
        <td>A unique identifier for each person record. IDs starting with P indicate passengers. IDs starting with O indicate a person who was not a passenger in the vehicle (e.g., driver, pedestrian, cyclist, etc.). </td>
        <td>O1414404 </td>
    </tr>
    <tr>
        <td>CRASH_ID </td>
        <td>Text </td>
        <td>128 </td>
        <td>This number can be used to link to the same crash in the Crashes and People datasets. This number also serves as a unique ID in the Crashes dataset. </td>
        <td>38328abee0afd4af28 19a18fd1bd4cddf1187160403c3d8 6c20895c7b33ec20627 0ced764c46e64e10 1608b77fadb019ce a75681cde0a4b7ab07b43ff1ebc4c6 </td>
    </tr>
    <tr>
        <td>CRASH_DATE </td>
        <td>Date/Time </td>
        <td>22 </td>
        <td>Date and time of crash as entered by the reporting officer </td>
        <td>9/6/22 23:55</td>
    </tr>
    <tr>
        <td>PERSON_TYPE </td>
        <td>Text </td>
        <td>10 </td>
        <td>Type of roadway user involved in crash </td>
        <td>DRIVER </td>
    </tr>
    <tr>
        <td>VEHICLE_ID </td>
        <td>Integer </td>
        <td>10 </td>
        <td>Unique id for the vehicle </td>
        <td>1344101 </td>
    </tr>
    <tr>
        <td>PERSON_SEX </td>
        <td>Text </td>
        <td>1 </td>
        <td>Gender of person involved in crash, as determined by reporting officer </td>
        <td>M </td>
    </tr>
    <tr>
        <td>PERSON_AGE </td>
        <td>Integer </td>
        <td>3 </td>
        <td>Age of person involved in crash </td>
        <td>47</td>
    </tr>
</table>

## Time Data Schema
<table>
    <tr>
        <td>Field Name </td>
        <td>Data Type </td>
        <td>Field Size for display </td>
        <td>Description </td>
        <td>Example </td>
    </tr>
    <tr>
        <td>CRASH_DATE </td>
        <td>Date/Time </td>
        <td>22 </td>
        <td>Date and time of crash as entered by the reporting officer </td>
        <td>9/6/22 23:55</td>
    </tr>
    <tr>
        <td>CRASH_ID </td>
        <td>Text </td>
        <td>128 </td>
        <td>This number can be used to link to the same crash in the Crashes and People datasets. This number also serves as a unique ID in the Crashes dataset. </td>
        <td>38328abee0afd4af28 19a18fd1bd4cddf1187160403c3d8 6c20895c7b33ec20627 0ced764c46e64e10 1608b77fadb019ce a75681cde0a4b7ab07b43ff1ebc4c6 </td>
    </tr>
    <tr>
        <td>CRASH_HOUR </td>
        <td>Integer </td>
        <td>10 </td>
        <td>The hour of the day component of CRASH_DATE. </td>
        <td>23 </td>
    </tr>
    <tr>
        <td>CRASH_DAY_OF_WEEK </td>
        <td>Integer </td>
        <td>10 </td>
        <td>The day of the week component of CRASH_DATE. Sunday=1 </td>
        <td>1 </td>
    </tr>
    <tr>
        <td>CRASH_MONTH </td>
        <td>Integer </td>
        <td>10 </td>
        <td>The month component of CRASH_DATE. </td>
        <td>9 </td>
    </tr>
    <tr>
        <td>DATE_POLICE_NOTIFIED </td>
        <td>Date/Time </td>
        <td>22 </td>
        <td>Calendar date on which police were notified of the crash </td>
        <td>9/11/22 23:50</td>
    </tr>
</table>

## Crash Data Schema
<table>
    <tr>
        <td>Field Name </td>
        <td>Data Type </td>
        <td>Field Size for display </td>
        <td>Description </td>
        <td>Example </td>
    </tr>
    <tr>
        <td>CRASH_UNIT_ID </td>
        <td>Integer </td>
        <td>10 </td>
        <td>A unique identifier for each vehicle record. </td>
        <td>1414411 </td>
    </tr>
    <tr>
        <td>CRASH_ID </td>
        <td>Text </td>
        <td>128 </td>
        <td>This number can be used to link to the same crash in the Crashes and People datasets. This number also serves as a unique ID in the Crashes dataset. </td>
        <td>38328abee0afd4af28 19a18fd1bd4cddf1187160403c3d8 6c20895c7b33ec20627 0ced764c46e64e10 1608b77fadb019ce a75681cde0a4b7ab07b43ff1ebc4c6 </td>
    </tr>
    <tr>
        <td>PERSON_ID </td>
        <td>Text </td>
        <td>10 </td>
        <td>A unique identifier for each person record. IDs starting with P indicate passengers. IDs starting with O indicate a person who was not a passenger in the vehicle (e.g., driver, pedestrian, cyclist, etc.). </td>
        <td>O1414404 </td>
    </tr>
    <tr>
        <td>VEHICLE_ID </td>
        <td>Integer </td>
        <td>10 </td>
        <td>Unique id for the vehicle </td>
        <td>1344101 </td>
    </tr>
    <tr>
        <td>NUM_UNITS </td>
        <td>Number </td>
        <td>100 </td>
        <td>Number of units involved in the crash. A unit can be a motor vehicle, a pedestrian, a bicyclist, or another non-passenger roadway user. Each unit represents a mode of traffic with an independent trajectory. </td>
        <td>3  </td>
    </tr>
    <tr>
        <td>TOTAL _INJURIES </td>
        <td>Number </td>
        <td>50 </td>
        <td>Total persons sustaining fatal, incapacitating, non-incapacitating, and possible injuries as determined by the reporting officer </td>
        <td>1</td>
    </tr>
</table>