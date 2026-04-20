Public d As Scripting.Dictionary

Sub init() '<-- initialize your dictionary once
    Set d = New Scripting.Dictionary
' Build a Dictionary of van IDs keyed on VRN
    d.Add Key:="HX21XMZ", Item:=419413
    d.Add Key:="HX22VOM", Item:=385151
    d.Add Key:="HK23ELO", Item:=441082
    d.Add Key:="HK73FFH", Item:=459362
    d.Add Key:="WX23UHK", Item:=430213
    d.Add Key:="HN23AUO", Item:=436802
    d.Add Key:="HK22BLF", Item:=402274
    d.Add Key:="HX23WDJ", Item:=436800
    d.Add Key:="HX23WCG", Item:=427647
    d.Add Key:="HN23XKU", Item:=436801
    d.Add Key:="HK21ULE", Item:=356694
    d.Add Key:="HX71WML", Item:=362984
    d.Add Key:="HX73WEP", Item:=451521
    d.Add Key:="HK22BLJ", Item:=402275
    d.Add Key:="HX21XMV", Item:=352895
    d.Add Key:="HX23WBN", Item:=430210
    d.Add Key:="HN23XML", Item:=441081
    d.Add Key:="HX71WMM", Item:=362982
    d.Add Key:="HX71WMO", Item:=362983
    d.Add Key:="HX73WEK", Item:=451517
    d.Add Key:="HT23FOF", Item:=451519
    d.Add Key:="WX23UHL", Item:=430212
    d.Add Key:="HK73FFJ", Item:=459361
    d.Add Key:="HX23WEO", Item:=451518
    d.Add Key:="HX22VNS", Item:=379823
    d.Add Key:="HX21XMT", Item:=356699
    d.Add Key:="HX21XMU", Item:=352888
    d.Add Key:="SG72USW", Item:=423352
    d.Add Key:="HX22VOO", Item:=385150
    d.Add Key:="HX73WEU", Item:=451520
    d.Add Key:="H23WEO", Item:=451518
    d.Add Key:="HX24UMU", Item:=468202
    d.Add Key:="HX19XEO", Item:=310546
    d.Add Key:="HX21XMW", Item:=356695
    d.Add Key:="HX24UMW", Item:=468201
    d.Add Key:="HX69WBW", Item:=322163
    d.Add Key:="WO19VPT", Item:=315099
    d.Add Key:="WO19VPU", Item:=314833
    d.Add Key:="EA69TLK", Item:=322162
    d.Add Key:="HN22OCS", Item:=404374
    d.Add Key:="HX23WEP", Item:=451521
    d.Add Key:="WP25KAA", Item:=519639
    d.Add Key:="WP25 KRG", Item:=518466
    d.Add Key:="WP25 KFJ", Item:=518467
    d.Add Key:="WP25 KFT", Item:=518464
    d.Add Key:="WP25 KNF", Item:=519653
    d.Add Key:="WP25 KFR", Item:=518465
    d.Add Key:="WP25 KRF", Item:=518462
    Call fetchRAMData
End Sub
Sub fetchRAMData()
    Dim hReq As Object
    Dim sht As Worksheet
    Dim authKey As String
    Dim arr As Object 'Parse the json array into here
    Dim json As String
    Dim jsonObj As Object
    Dim i As Integer
    Dim jobData As Variant
    Dim rowData As Variant
    Dim rng As Range
    Dim jsonData As String
    Dim authObj As Object
    Dim myAuth As String
    Dim fileNum As Integer
    Dim DataLine As String
    Dim range_to_filter As Range
    Set range_to_filter = Range("B1")
    Set sht = Sheet1
    sht.Cells.Clear
    Dim oSh As Worksheet
    Dim lo As ListObject
    Dim myArray As Variant
    Dim rng2 As Range
    Dim myString As String
    Dim myString2 As String
    Dim theDate As Variant
    Dim dateFrom As Variant
    Dim dateTo As Variant
    Dim strUrl As String
    Dim vrn As String
    
   
    'theDate = CalendarForm.GetDate
    theDate = thisDate
    dateFrom = Format(DateAdd("d", -1, theDate), "yyyy-mm-dd")
    dateTo = Format(DateAdd("d", 3, theDate), "yyyy-mm-dd")
    
' get the unique VRNs from the master sheet

   ' Collect VRNs from columns C, D, E (Shunt, Delivery, Return)
    Dim valuesC As Variant
    Dim valuesD As Variant
    Dim valuesE As Variant
    valuesC = ActiveSheet.Range("C:C").Value2
    valuesD = ActiveSheet.Range("D:D").Value2
    valuesE = ActiveSheet.Range("E:E").Value2

    ' Add a reference to Microsoft Scripting Runtime
    Dim dic As Scripting.Dictionary
    Set dic = New Scripting.Dictionary

    ' Set the comparison mode to case-sensitive
    dic.CompareMode = BinaryCompare

    ' Headers to exclude
    Dim headers As Scripting.Dictionary
    Set headers = New Scripting.Dictionary
    headers.Add "Shunt Vehicle", True
    headers.Add "Delivery Vehicle", True
    headers.Add "Return Vehicle", True

    Dim r As Long
    Dim candidate As Variant

    ' Add from column C
    For r = LBound(valuesC, 1) To UBound(valuesC, 1)
        candidate = valuesC(r, 1)
        If Not IsEmpty(candidate) Then
            If Not headers.exists(CStr(candidate)) Then
                If Not dic.exists(CStr(candidate)) Then dic.Add CStr(candidate), 0
            End If
        End If
    Next r

    ' Add from column D
    For r = LBound(valuesD, 1) To UBound(valuesD, 1)
        candidate = valuesD(r, 1)
        If Not IsEmpty(candidate) Then
            If Not headers.exists(CStr(candidate)) Then
                If Not dic.exists(CStr(candidate)) Then dic.Add CStr(candidate), 0
            End If
        End If
    Next r

    ' Add from column E
    For r = LBound(valuesE, 1) To UBound(valuesE, 1)
        candidate = valuesE(r, 1)
        If Not IsEmpty(candidate) Then
            If Not headers.exists(CStr(candidate)) Then
                If Not dic.exists(CStr(candidate)) Then dic.Add CStr(candidate), 0
            End If
        End If
    Next r

    'Extract the dictionary's keys as a 1D array
    Dim result As Variant
    result = dic.Keys
    On Error Resume Next
    For Each Element In result
        If Element <> "" Then
            vrn = Element
            ActiveWorkbook.Sheets.Add After:=Worksheets(Worksheets.Count)
            ActiveWorkbook.Sheets(Worksheets.Count).name = vrn


            Call get_Token
            Application.Wait (Now + TimeValue("0:00:05"))
            Set oSh = ActiveSheet
            fileNum = FreeFile()
            Open "c:\tmp\result.json" For Input As #fileNum
            While Not EOF(fileNum)
                Line Input #fileNum, DataLine
                'Debug.Print DataLine
                jsonData = DataLine
                Set authObj = JsonConverter.ParseJson(jsonData)
            Wend
            authKey = authObj("access_token")
            strUrl = "https://api.qaifn.co.uk/api/v1/history/" & d(vrn) & "/" & dateFrom & "T00:00:00/" & dateTo & "T00:00:00"
            'Debug.Print strUrl
            'Debug.Print authKey
            Set hReq = CreateObject("MSXML2.XMLHTTP")
            With hReq
                .Open "GET", strUrl, False
                .SetRequestHeader "Authorization", "Bearer " & authKey
                .Send
            End With
        
            Dim response As String
                json = hReq.responseText
                Set jsonObj = JsonConverter.ParseJson(json)
                Range("A1").value = "Event Date"
                Range("B1").value = "Event Name"
                Range("C1").value = "Post Code"
                Range("D1").value = "Speed"
                Range("E1").value = "Speed Limit"
                Range("F1").value = "Odometer"
                Range("G1").value = "Registration"
                Range("H1").value = "Latitude"
                Range("I1").value = "Longitude"
                Range("J1").value = "Category"
                Range("K1").value = "Mileage"
                Range("L1").value = "Time"
                If jsonObj("history").Count > 0 Then
                    For i = 1 To jsonObj("history").Count
                        Range("A" & i + 1).value = jsonObj("history")(i)("event_date")
                        Range("B" & i + 1).value = jsonObj("history")(i)("event_name")
                        Range("C" & i + 1).value = jsonObj("history")(i)("postCode")
                        Range("D" & i + 1).value = jsonObj("history")(i)("speedKph")
                        Range("E" & i + 1).value = jsonObj("history")(i)("speedLimitKph")
                        Range("F" & i + 1).value = jsonObj("history")(i)("odometer")
                        Range("G" & i + 1).value = jsonObj("registration")
                        Range("H" & i + 1).value = jsonObj("history")(i)("latitude")
                        Range("I" & i + 1).value = jsonObj("history")(i)("longitude")
                    Next i
        
                    With oSh
                        lastRow = .Cells(.Rows.Count, "A").End(xlUp).row
                    End With
                    Set rng = Range(Range("A1"), Range("L" & lastRow))
                    Set tbl = ActiveSheet.ListObjects.Add(xlSrcRange, rng, , xlYes)
                    tbl.TableStyle = "TableStyleLight15"
                    
                    tbl.name = vrn
                    
                    ' filter here
                    'range_to_filter.AutoFilter Field:=2, Criteria1:="GEOFENCE_OUT", Criteria2:="GEOFENCE_IN"
                Else
                    Range("B3").value = "NO DATA AVAILABLE"
                End If
        
                Range("A2:K2").EntireColumn.AutoFit
        End If
    Next Element
'
'                Set lo = oSh.ListObjects(1)
'
'        ' delete rows which aren't GEOFENCE_IN or GEOFENCE_OUT
'                myString = "GEOFENCE"
'                With lo.DataBodyRange.Columns(2)
'                    For RW = .Rows.Count To 1 Step -1
'                        If InStr(1, .Cells(RW).Value2, myString) = 0 Then
'                            tbl.ListRows(RW).Delete
'                        End If
'                    Next RW
'                End With
'                myString2 = "GEOFENCE_IN"
'
'        ' Set the GEOFENCE codes
'
'                myPC1 = "TR9" ' FlyingFish HQ
'                myPC2 = "SN5" ' Swindon Depot
'                myPC3 = "B69" ' Birmingham Depot
'                myPC4 = "TR18" ' Newlyn Fish Market
'                myPC5 = "TQ5" ' Brixham Fish Market
'
'        ' set by following loop, but defaults to Swindon
'                myPCx = "SN5" ' default
'
'        ' establish which route/shunt is involved
'                With lo.DataBodyRange.Columns(3)
'                    For RW = .Rows.Count To 1 Step -1
'                        If InStr(1, .Cells(RW).Value2, myPC1) > 0 Then
'                            If InStr(1, .Cells(RW + 1).Value2, myPC2) > 0 Then
'                                myPCx = "SN5"
'                            ElseIf InStr(1, .Cells(RW + 1).Value2, myPC3) > 0 Then
'                                myPCx = "B69"
'                            ElseIf InStr(1, .Cells(RW + 1).Value2, myPC4) > 0 Then
'                                myPCx = "TR18"
'                            ElseIf InStr(1, .Cells(RW + 1).Value2, myPC5) > 0 Then
'                                myPCx = "TQ5"
'                            End If
'                        End If
'                    Next RW
'                End With
'
'                Select Case myPCx
'                    Case "TR9"
'                        'Debug.Print "Home Base"
'                    Case "SN5"
'                        'Debug.Print "Swindon depot"
'                    Case "B69"
'                        'Debug.Print "Birmingham depot"
'                    Case "TR18"
'                        'Debug.Print "Newlyn market"
'                    Case "TQ5"
'                        'Debug.Print "Brixham market"
'                    Case Else
'                        'Debug.Print "Not found"
'                End Select
'
'        ' pull the appropriate data from the sheet and calculate time and distance
'        On Error Resume Next
'                With lo.DataBodyRange.Columns(3)
'                    For RW = .Rows.Count To 1 Step -1
'                        If InStr(1, .Cells(RW).Value2, myPC1) > 0 Then
'                            If InStr(1, .Cells(RW + 1).Value2, myPCx) > 0 Then
'                                If lo.DataBodyRange.Columns(2).Cells(RW + 1) = myString2 Then
'                                    .Cells(RW).EntireRow.Interior.ColorIndex = 35
'                                    .Cells(RW + 1).EntireRow.Interior.ColorIndex = 35
'                                    If myPCx = "TQ5" Or myPCx = "TR18" Then
'                                        Range("J" & RW + 1).Value = "Out"
'                                        Range("J" & RW + 2).Value = "Out"
'                                    Else
'                                        Range("J" & RW + 1).Value = Range("A" & RW + 1).Value
'                                        Range("J" & RW + 2).Value = Range("A" & RW + 2).Value
'                                    End If
'                                    ' look at the next GEOFENCE_OUT/GEOFENCE_IN pair
'                                    ' i suspect this If statement is where it's going wrong
'                                    'If InStr(1, .Cells(RW + 3).Value2, myPCx) > 0 And InStr(1, .Cells(RW + 4).Value2, myPCx) > 0 Then
'                                    ' if the postcodes are both Swindon and the datetime of the _IN is the date of the _OUT + 1 mark as "delivery start/end
'                                         If DateAdd("d", 1, DateValue(Range("A" & RW + 3))) = DateValue(Range("A" & RW + 4)) Then
'                                            .Cells(RW + 2).EntireRow.Interior.ColorIndex = 36
'                                            .Cells(RW + 3).EntireRow.Interior.ColorIndex = 36
'                                            Range("J" & RW + 3).Value = Range("A" & RW + 3).Value
'                                            Range("J" & RW + 4).Value = Range("A" & RW + 4).Value
'                                            ' calculate the mileage
'                                            Range("K" & RW + 4).Value = lo.DataBodyRange.Columns(6).Cells(RW + 3) - lo.DataBodyRange.Columns(6).Cells(RW)
'                                            ' calculate the time
'                                            t1 = Range("A" & RW + 4).Value
'                                            t2 = Range("A" & RW + 1).Value
'                                            If t1 > 0 Then
'                                                Range("L" & RW + 4).Value = t1 - t2
'                                                Range("L" & RW + 4).NumberFormat = "[h]:mm:ss"
'                                            End If
'                                        ' otherwise proceed to the next pair and repeat the check
'                                         ElseIf DateAdd("d", 1, DateValue(Range("A" & RW + 5))) = DateValue(Range("A" & RW + 6)) Then
'                                            .Cells(RW + 4).EntireRow.Interior.ColorIndex = 36
'                                            .Cells(RW + 5).EntireRow.Interior.ColorIndex = 36
'                                            Range("J" & RW + 5).Value = "Delivery Start"
'                                            Range("J" & RW + 6).Value = "Delivery End"
'                                            ' calculate the mileage
'                                            Range("K" & RW + 6).Value = lo.DataBodyRange.Columns(6).Cells(RW + 5) - lo.DataBodyRange.Columns(6).Cells(RW)
'                                            ' calculate the time
'                                            t1 = Range("A" & RW + 6).Value
'                                            t2 = Range("A" & RW + 1).Value
'                                            If t1 > 0 Then
'                                                Range("L" & RW + 6).Value = t1 - t2
'                                                Range("L" & RW + 6).NumberFormat = "[h]:mm:ss"
'                                            End If
'                                         Else
'                                            ' error
'                                            Debug.Print "Date Error"
'                                         End If
'                                    'End If
'                                End If
'                            End If
'                        End If
'                    Next RW
'                End With
'        ' repeat the whole process to find the return journeys
'                myPC2 = "TR9"
'                With lo.DataBodyRange.Columns(3)
'                    For RW = .Rows.Count To 1 Step -1
'                        If InStr(1, .Cells(RW).Value2, myPCx) > 0 Then
'                            If InStr(1, .Cells(RW + 1).Value2, myPC2) > 0 Then
'                                If lo.DataBodyRange.Columns(2).Cells(RW + 1) = myString2 Then
'                                    .Cells(RW).EntireRow.Interior.ColorIndex = 37
'                                    .Cells(RW + 1).EntireRow.Interior.ColorIndex = 37
'                                    If myPCx = "TQ5" Or myPCx = "TR18" Then
'                                        Range("J" & RW + 1).Value = "Return"
'                                        Range("J" & RW + 2).Value = "Return"
'                                    Else
'                                        Range("J" & RW + 1).Value = Range("A" & RW + 1).Value
'                                        Range("J" & RW + 2).Value = Range("A" & RW + 2).Value
'                                    End If
'                                    ' calculate the mileage
'                                    Range("K" & RW + 2).Value = lo.DataBodyRange.Columns(6).Cells(RW + 2) - lo.DataBodyRange.Columns(6).Cells(RW)
'                                    ' calculate the time
'                                    t1 = Range("A" & RW + 2).Value
'                                    t2 = Range("A" & RW + 1).Value
'                                    If t1 > 0 Then
'                                        Range("L" & RW + 2).Value = t1 - t2
'                                        Range("L" & RW + 2).NumberFormat = "[h]:mm:ss"
'                                    End If
'                                End If
'                            End If
'                        End If
'                    Next RW
'                End With
'        '   Find last row in table
'            lastRow = lo.Range.Rows.Count
'        End If
'    Next Element

End Sub
Function get_Token()
    Dim curlCommand As String
    ' Construct the CURL command
    curlCommand = "curl -X POST --user ""Flyingfish:c86c85cc-1608-4dcb-bb28-47592d212160"" -d ""grant_type=password&username=Flyingfish&password=Sprinter2025$"" https://auth.qaifn.co.uk/oauth/token > c:\tmp\result.json"
    Shell "cmd.exe /s /k " & curlCommand & " && exit"
End Function

Sub CopyMatchingData()
    ' Declare variables
    Dim mainSheet As Worksheet
    Dim targetSheet As Worksheet
    Dim i As Long, j As Long
    Dim lastRowMain As Long, lastRowTarget As Long
    Dim matchFound As Boolean
    Dim sheetExists As Boolean
    
    ' Set main worksheet reference
    Set mainSheet = ActiveWorkbook.Worksheets("Main")
    
    ' Find last row with data in Main sheet
    lastRowMain = mainSheet.Cells(mainSheet.Rows.Count, "A").End(xlUp).row
    
    Application.ScreenUpdating = False ' Speed up execution
    
    ' Loop through each row in Main sheet
    For i = 2 To lastRowMain ' Assuming row 1 has headers
        ' Reset match flag
        matchFound = False
        
        ' Get date from column A of Main sheet
        Dim dateMain As Variant
        dateMain = mainSheet.Cells(i, 1).value
        
        ' Skip if not a valid date
        If Not IsDate(dateMain) Then
            GoTo ApplyNoValue
        End If
        
        ' Get target sheet name from column D of Main sheet
        Dim targetSheetName As String
        targetSheetName = mainSheet.Cells(i, 4).value
        
        ' Check if the sheet exists
        sheetExists = False
        On Error Resume Next
        Set targetSheet = ActiveWorkbook.Worksheets(targetSheetName)
        If Not targetSheet Is Nothing Then sheetExists = True
        On Error GoTo 0
        
        ' Skip if sheet doesn't exist
        If Not sheetExists Then
            GoTo ApplyNoValue
        End If
        
        ' Find last row with data in target sheet
        lastRowTarget = targetSheet.Cells(targetSheet.Rows.Count, "A").End(xlUp).row
        
        ' Loop through each row in target sheet
        For j = 2 To lastRowTarget ' Assuming row 1 has headers
            ' Get date from column A of target sheet
            Dim dateTarget As Variant
            dateTarget = targetSheet.Cells(j, 1).value
            
            ' Skip if not a valid date
            If Not IsDate(dateTarget) Then
                GoTo NextTargetRow
            End If
            
            ' Check if dates (ignoring time) match
            If DateValue(CDate(dateMain)) = DateValue(CDate(dateTarget)) Then
                ' Check if there are values in columns K and L
                If Not IsEmpty(targetSheet.Cells(j, 11).value) And Not IsEmpty(targetSheet.Cells(j, 12).value) Then
                    ' Copy values from target sheet columns K and L to Main sheet columns R and S
                    mainSheet.Cells(i, 18).value = targetSheet.Cells(j, 11).value ' Column R = Column K
                    mainSheet.Cells(i, 19).value = targetSheet.Cells(j, 12).value ' Column S = Column L
                    matchFound = True
                    Exit For ' No need to check further rows in target sheet
                End If
            End If
NextTargetRow:
        Next j
        
        ' If no match found, add "no value" to columns R and S
        If Not matchFound Then
ApplyNoValue:
            mainSheet.Cells(i, 18).value = ""
            mainSheet.Cells(i, 19).value = ""
        End If
    Next i
    
    Application.ScreenUpdating = True
    
    MsgBox "Data copying complete!", vbInformation
End Sub



