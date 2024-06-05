Imports System.Net
Imports System.IO
Imports Newtonsoft.Json
Imports Newtonsoft
Imports System.Net.Mail
Imports System.Data.SqlClient
Imports Microsoft.VisualBasic.FileIO
Imports ClassiC4C
Imports System.Timers

Module Module1
    Dim List_Cust As New Dictionary(Of String, result_account)
    Dim Wc_Cust As New WC_DataRow
    Dim Dati_Cust As New Dati_C4C_Accont
    Dim g_streamlog As StreamWriter



    Sub Writelog(ByVal Msg As String)
        g_streamlog.WriteLine(Format(Now, "HH:mm:ss") & " " & Msg)
        g_streamlog.Flush()
        Console.WriteLine(Format(Now, "HH:mm:ss") & " " & Msg)
    End Sub

    Sub Main()
        Dim Wc As New ClassiC4C.WC_DataRow
        Dim Wc_Btd As New ClassiC4C.WC_DataRow
        Dim Wc_Email As New ClassiC4C.WC_DataRow
        Dim Wc_Cont As New ClassiC4C.WC_DataRow
        Dim Wc_PersCont As New ClassiC4C.WC_DataRow
        Dim Wc_Upd As New WC_DataRow
        Dim Wc_UpdSr As New WC_DataRow
        Dim Wc_Srt As New WC_DataRow
        Dim Wc_ESP As New WC_DataRow
        Dim Wc_Ctxt As New WC_DataRow
        Dim Dati_Cont As New Dati_C4C_Contatto
        Dim l_Cont_Spec As String
        Dim L_TxTEmail As String = ""
        Dim Id_NoLogo As String = "1149037"


        Dim T_start As Date
        T_start = Now

        Try
            If Dir(My.Application.Info.DirectoryPath & "\log", vbDirectory) = "" Then
                MkDir(My.Application.Info.DirectoryPath & "\log")
            End If
            g_streamlog = New StreamWriter(My.Application.Info.DirectoryPath & "\log\" & Format(Now, "yyyyMMdd") & ".log", True)
            g_streamlog.AutoFlush = True
        Catch ex As Exception
            Exit Sub
        End Try


        Dim T_Email As New DataTable
        T_Email.Columns.Add("email")
        T_Email.Columns.Add("conteggio")
        Dim T_K As DataColumn()
        ReDim T_K(1)
        T_K(0) = T_Email.Columns("email")
        T_Email.PrimaryKey = T_K
        T_Email.TableName = "T_Email"



        Dim Tab_Domain As New DataTable
        Tab_Domain.Columns.Add("dominio")
        Tab_Domain.Columns.Add("account")
        Tab_Domain.Columns.Add("nome")
        Tab_Domain.Columns.Add("citta")
        Tab_Domain.Columns.Add("tipo")
        Tab_Domain.Columns.Add("stato")
        Tab_Domain.Columns.Add("area")
        Tab_Domain.Columns.Add("piva")
        Tab_Domain.Columns.Add("codsap")
        ReDim T_K(2)
        T_K(0) = Tab_Domain.Columns("dominio")
        T_K(1) = Tab_Domain.Columns("account")
        Tab_Domain.PrimaryKey = T_K
        Tab_Domain.TableName = "T_Dominio"


        Dim Tab_Spec As New DataTable
        Tab_Spec.Columns.Add("email")
        Tab_Spec.Columns.Add("soggetto")
        Tab_Spec.Columns.Add("contatto")
        ReDim T_K(2)
        T_K(0) = Tab_Spec.Columns("email")
        T_K(1) = Tab_Spec.Columns("soggetto")
        Tab_Spec.PrimaryKey = T_K
        Tab_Spec.TableName = "T_Spec"

        If Dir("Dati\Tab_Spec.xml") <> "" Then
            Tab_Spec.ReadXml("Dati\Tab_Spec.xml")
        End If



        If Dir("T_Email.xml") <> "" Then
            T_Email.ReadXml("T_Email.xml")
        End If


        If Dir("Tab_Domain.xml") <> "" Then
            Tab_Domain.ReadXml("Tab_Domain.xml")
        End If



        Dim Tab_DomExclude As New DataTable
        Tab_DomExclude.Columns.Add("dominio")
        ReDim T_K(1)
        T_K(0) = Tab_DomExclude.Columns("dominio")
        Tab_DomExclude.PrimaryKey = T_K
        Tab_DomExclude.TableName = "Tab_DomExclude"



        If Dir("Dati\Tab_DomExclude.xml") <> "" Then
            Tab_DomExclude.ReadXml("Dati\Tab_DomExclude.xml")
        End If


        Dim T_s As New JsonSerializerSettings
        T_s.NullValueHandling = NullValueHandling.Ignore


        Wc.Skip = 0
        Wc.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc.Server = My.Settings.SERVER
        Wc.Collection = "ServiceRequestCollection"

        Wc.Filtro = "BuyerPartyID eq '" & Id_NoLogo & "' and ServiceRequestUserLifeCycleStatusCode ne '5' and " &
                                    "CreationDateTime ge datetimeoffset'" & Format(Now.AddDays(-1), "yyyy-MM-ddT00:00:00.000Z") & "'"
        'Wc.Filtro = "BuyerPartyID eq '" & Id_NoLogo & "' and ID eq '99480' "
        Wc.Ordine = "ID DESC"

        Wc_Btd.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, "Reda2005")
        Wc_Btd.Server = My.Settings.SERVER
        Wc_Btd.Collection = "EMailBTDReferenceCollection"


        Wc_Email.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_Email.Server = My.Settings.SERVER
        Wc_Email.Collection = "EMailCollection"


        Wc_Cont.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_Cont.Server = My.Settings.SERVER
        Wc_Cont.Collection = "ContactCollection"


        Wc_PersCont.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_PersCont.Server = My.Settings.SERVER
        Wc_PersCont.Collection = "ContactPersonalAddressCollection"

        Wc_Upd.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_Upd.Server = My.Settings.SERVER
        Wc_Upd.Collection = "ContactCollection"

        Wc_UpdSr.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_UpdSr.Server = My.Settings.SERVER
        Wc_UpdSr.Collection = "ServiceRequestCollection"

        Wc_Srt.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_Srt.Server = My.Settings.SERVER
        Wc_Srt.Collection = "ServiceRequestTextCollectionCollection"

        Wc_Cust.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_Cust.Server = My.Settings.SERVER
        Wc_Cust.Collection = "CorporateAccountCollection"

        Wc_ESP.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_ESP.Server = My.Settings.SERVER
        Wc_ESP.Collection = "EMailSenderPartyCollection"


        Wc_Ctxt.Credenziali = New NetworkCredential(My.Settings.AUTH_USERNAME, My.Settings.AUTH_PASSWORD)
        Wc_Ctxt.Server = My.Settings.SERVER
        Wc_Ctxt.Collection = "ContactTextCollection"



        Dim U_Contatto As New result_contatto

        Do
            Wc.Elabora()

            If Not Wc.Err Then
                Dim Dati_ServiceRequest As New Dati_C4C_ServiceRequest
                Dati_ServiceRequest = JsonConvert.DeserializeObject(Of Dati_C4C_ServiceRequest)(Wc.Result)

                For Each L_SR As result_ServiceRequest In Dati_ServiceRequest.d.results
                    Wc_Btd.Filtro = "ID eq '" & L_SR.ID & "'"
                    Wc_Btd.Elabora()
                    Dim Dati_BTD As New Dati_C4C_EMailBTDReference
                    Dati_BTD = JsonConvert.DeserializeObject(Of Dati_C4C_EMailBTDReference)(Wc_Btd.Result)
                    For Each L_BTD As result_EMailBTDReference In Dati_BTD.d.results
                        Writelog(L_SR.ID & " " & L_BTD.EMailID)
                        Wc_Email.Filtro = "ID eq '" & L_BTD.EMailID & "'"
                        Wc_Email.Elabora()
                        Dim Dati_Email As New Dati_C4C_EMail
                        Dati_Email = JsonConvert.DeserializeObject(Of Dati_C4C_EMail)(Wc_Email.Result)
                        For Each L_Email As result_EMail In Dati_Email.d.results
                            Dim T_Email_Addr As String = LCase(L_Email.MessageFromEmailURI)
                            l_Cont_Spec = ""
                            For Each R_Spec As DataRow In Tab_Spec.Select("email='" & T_Email_Addr & "'")
                                If R_Spec("soggetto") = L_SR.Name Then
                                    l_Cont_Spec = R_Spec("contatto").ToString
                                End If
                                If Mid(R_Spec("soggetto"), 1, 1) = "*" Then

                                    If Right(L_SR.Name, R_Spec("soggetto").ToString.Length - 1) = Mid(R_Spec("soggetto"), 2) Then
                                        l_Cont_Spec = R_Spec("contatto").ToString
                                    End If
                                End If
                            Next

                            Writelog(L_SR.ID & " " & T_Email_Addr & " " & l_Cont_Spec)
                            If T_Email_Addr.Contains("@") Then
                                Dim T_account As String = ""
                                Dim T_Valido As Boolean = True
                                Dim T_domain As String = LCase(Split(T_Email_Addr, "@")(1))

                                If T_domain = "elesa.com" Then
                                    Wc_Srt.Filtro = "ServiceRequestID eq '" & L_SR.ID & "'"
                                    Wc_Srt.Elabora()
                                    Dim Dati_Srt As New Dati_C4C_ServiceRequestTextCollection
                                    Dati_Srt = JsonConvert.DeserializeObject(Of Dati_C4C_ServiceRequestTextCollection)(Wc_Srt.Result)
                                    For Each R_Srt As result_ServiceRequestTextCollection In Dati_Srt.d.results
                                        L_TxTEmail = R_Srt.FormattedText
                                        Dim t_txt As String() = R_Srt.FormattedText.Split("<")
                                        For i = 0 To t_txt.Length - 1
                                            t_txt(i) = LCase(t_txt(i).Replace("br>", ""))
                                            t_txt(i) = LCase(t_txt(i).Replace("&nbsp;", ""))
                                            If t_txt(i).Contains("@") Then
                                                t_txt(i) = t_txt(i).Replace(vbTab, "")
                                                t_txt(i) = t_txt(i).Replace("'", " ")
                                                t_txt(i) = t_txt(i).Replace("&gt;", " ")
                                                t_txt(i) = t_txt(i).Replace("&lt;", " ")
                                                t_txt(i) = t_txt(i).Replace("""", " ")
                                                t_txt(i) = t_txt(i).Replace(";", " ")
                                                t_txt(i) = t_txt(i).Replace(",", " ")
                                                t_txt(i) = t_txt(i).Replace("(", " ")
                                                t_txt(i) = t_txt(i).Replace(")", " ")
                                                t_txt(i) = t_txt(i).Replace("<", " ")
                                                t_txt(i) = t_txt(i).Replace(">", " ")
                                                Dim t_txt2 As String() = t_txt(i).Split(" ")
                                                For j As Integer = 0 To t_txt2.Length - 1
                                                    If t_txt2(j).Contains("@") And T_domain = "elesa.com" Then
                                                        t_txt2(j) = LCase(t_txt2(j).Replace("[mailto", "").Replace("]", "").Replace(":", ""))
                                                        If Not t_txt2(j).StartsWith("[cid") Then
                                                            'Writelog("**" & t_txt(i))
                                                            Writelog(T_Email_Addr & " -> " & t_txt2(j))
                                                            T_Email_Addr = t_txt2(j)
                                                            T_domain = LCase(Split(T_Email_Addr, "@")(1))
                                                        End If
                                                    End If
                                                Next

                                            End If

                                        Next
                                        'If T_domain = "elesa.com" Then
                                        '    Writelog("=========================================")
                                        '    For i = 0 To t_txt.Length - 1
                                        '        If t_txt(i).Contains("@") Then
                                        '            Writelog(t_txt(i))
                                        '        End If
                                        '    Next
                                        '    Writelog("=========================================")
                                        'End If

                                    Next

                                End If




                                If T_Email.Select("email='" & Replace(T_Email_Addr, "'", "''") & "'").Length = 0 Or l_Cont_Spec <> "" Then
                                    If T_domain <> "elesa.com" Then
                                        U_Contatto = New result_contatto
                                        U_Contatto.ContactID = ""


                                        If l_Cont_Spec <> "" Then
                                            Wc_Cont.Filtro = "ContactID eq '" & l_Cont_Spec & "' and StatusCode eq '2'"
                                        Else
                                            Wc_Cont.Filtro = "endswith(Email,'@" & T_domain & "') and StatusCode eq '2'"
                                            If Tab_DomExclude.Select("dominio='" & T_domain & "'").Length <> 0 Then
                                                Wc_Cont.Filtro = "Email eq '" & T_Email_Addr & "' and StatusCode eq '2'"
                                            End If
                                        End If




                                        Wc_Cont.Elabora()
                                        If Not Wc_Cont.Err Then
                                            Dati_Cont = JsonConvert.DeserializeObject(Of Dati_C4C_Contatto)(Wc_Cont.Result)
                                            For Each R_Cont As result_contatto In Dati_Cont.d.results
                                                If (Not Mid(R_Cont.Email, 1, 1) = "#" Or l_Cont_Spec <> "") And U_Contatto.ContactID = "" Then
                                                    If LCase(R_Cont.Email) = T_Email_Addr Or l_Cont_Spec <> "" Then
                                                        U_Contatto.ContactID = R_Cont.ContactID
                                                        U_Contatto.AccountID = R_Cont.AccountID
                                                    Else
                                                        Dim R_Cust As result_account = leggi_Cust(R_Cont.AccountID)
                                                        If Not R_Cust Is Nothing Then
                                                            If Mid(R_Cust.AreaClienti_KUT, 1, 2) = "IT" And
                                                                        R_Cust.LifeCycleStatusCode = "2" And
                                                                        Not Mid(R_Cust.Name, 1, 1) = "#" And
                                                                        R_Cust.ExternalID <> "33913" Then
                                                                If T_account = "" Then T_account = R_Cont.AccountID
                                                                If T_account <> R_Cont.AccountID Then
                                                                    T_Valido = False
                                                                End If
                                                                If Tab_Domain.Select("dominio='" & T_domain & "' and account='" & R_Cont.AccountID & "'").Length = 0 Then
                                                                    Dim L_TD As DataRow = Tab_Domain.NewRow
                                                                    L_TD("dominio") = T_domain
                                                                    L_TD("account") = R_Cont.AccountID
                                                                    Tab_Domain.Rows.Add(L_TD)
                                                                    Tab_Domain.WriteXml("Tab_Domain.xml")
                                                                End If
                                                            End If
                                                        End If
                                                    End If
                                                End If
                                            Next
                                        End If

                                        If U_Contatto.ContactID = "" Then
                                            Wc_PersCont.Filtro = "endswith(EMail,'@" & T_domain & "')"
                                            If Tab_DomExclude.Select("dominio='" & T_domain & "'").Length <> 0 Then
                                                Wc_PersCont.Filtro = "EMail eq '" & T_Email_Addr & "'"
                                            End If
                                            Wc_PersCont.Elabora()
                                            If Not Wc_PersCont.Err Then
                                                Dim Dati_PersCont As New Dati_C4C_PersonalAddress
                                                Dati_PersCont = JsonConvert.DeserializeObject(Of Dati_C4C_PersonalAddress)(Wc_PersCont.Result)
                                                For Each R_PersCont As result_PersonalAddress In Dati_PersCont.d.results
                                                    If Not Mid(R_PersCont.EMail, 1, 1) = "#" And U_Contatto.ContactID = "" Then
                                                        Wc_Cont.Filtro = "ContactID eq '" & R_PersCont.ContactID & "' and StatusCode eq '2'"
                                                        Wc_Cont.Elabora()
                                                        Dati_Cont = JsonConvert.DeserializeObject(Of Dati_C4C_Contatto)(Wc_Cont.Result)
                                                        For Each R_Cont As result_contatto In Dati_Cont.d.results
                                                            If LCase(R_Cont.Email) = T_Email_Addr Then
                                                                U_Contatto.ContactID = R_Cont.ContactID
                                                                U_Contatto.AccountID = R_Cont.AccountID
                                                            Else
                                                                Dim R_Cust As result_account = leggi_Cust(R_Cont.AccountID)
                                                                If Not R_Cust Is Nothing Then
                                                                    If Mid(R_Cust.AreaClienti_KUT, 1, 2) = "IT" And
                                                                                    R_Cust.LifeCycleStatusCode = "2" And
                                                                                    Not Mid(R_Cust.Name, 1, 1) = "#" And
                                                                                    R_Cust.ExternalID <> "33913" Then
                                                                        If T_account = "" Then T_account = R_Cont.AccountID
                                                                        If T_account <> R_Cont.AccountID Then
                                                                            T_Valido = False
                                                                        End If
                                                                        If Tab_Domain.Select("dominio='" & T_domain & "' and account='" & R_Cont.AccountID & "'").Length = 0 Then
                                                                            Dim L_TD As DataRow = Tab_Domain.NewRow
                                                                            L_TD("dominio") = T_domain
                                                                            L_TD("account") = R_Cont.AccountID
                                                                            Tab_Domain.Rows.Add(L_TD)
                                                                            Tab_Domain.WriteXml("Tab_Domain.xml")
                                                                        End If
                                                                    End If
                                                                End If
                                                            End If
                                                        Next
                                                    End If
                                                Next
                                            End If
                                        End If


                                        If U_Contatto.ContactID = "" Then
                                            Wc_Cust.Filtro = "endswith(Email,'@" & T_domain & "')"
                                            If Tab_DomExclude.Select("dominio='" & T_domain & "'").Length <> 0 Then
                                                Wc_Cust.Filtro = "Email eq '" & T_Email_Addr & "''"
                                            End If
                                            Wc_Cust.Elabora()
                                            If Not Wc_Cust.Err Then
                                                Dati_Cust = JsonConvert.DeserializeObject(Of Dati_C4C_Accont)(Wc_Cust.Result)
                                                For Each R_Cust As result_account In Dati_Cust.d.results
                                                    If Not Mid(R_Cust.Email, 1, 1) = "#" And Not Mid(R_Cust.Name, 1, 1) = "#" And
                                                                R_Cust.LifeCycleStatusCode = "2" And Mid(R_Cust.AreaClienti_KUT, 1, 2) = "IT" _
                                                                And R_Cust.ExternalID <> "33913" Then
                                                        If Tab_Domain.Select("dominio='" & T_domain & "' and account='" & R_Cust.AccountID & "'").Length = 0 Then
                                                            Dim L_TD As DataRow = Tab_Domain.NewRow
                                                            L_TD("dominio") = T_domain
                                                            L_TD("account") = R_Cust.AccountID
                                                            Tab_Domain.Rows.Add(L_TD)
                                                        End If
                                                        If T_account = "" Then T_account = R_Cust.AccountID
                                                        If T_account <> R_Cust.AccountID Then
                                                            T_Valido = False
                                                        End If
                                                    End If

                                                    If Not List_Cust.ContainsKey(R_Cust.AccountID) Then
                                                        List_Cust.Add(R_Cust.AccountID, R_Cust)
                                                    End If

                                                Next
                                            End If
                                        End If



                                        If T_Valido Then
                                            For Each L_TD As DataRow In Tab_Domain.Select("dominio='" & T_domain & "'")
                                                L_TD.Delete()
                                            Next
                                        Else
                                            Tab_Domain.WriteXml("Tab_Domain.xml")
                                        End If



                                        If T_account = "" Then
                                            T_Valido = False
                                        End If
                                        If Not U_Contatto.ContactID = "" Then
                                            T_Valido = True
                                        End If
                                        If T_account = Id_NoLogo Or U_Contatto.AccountID = Id_NoLogo Then
                                            T_Valido = False
                                        End If



                                        Dim Nome_utente As String = LCase(T_Email_Addr.Split("@")(0))

                                        Select Case Nome_utente
                                            Case "mailer-daemon", "postmaster", ""
                                                T_Valido = False
                                                T_account = "NO_CREATE"
                                        End Select

                                        If Nome_utente.Contains("[") Then
                                            T_Valido = False
                                            T_account = "NO_CREATE"
                                        End If


                                        Writelog(L_SR.ID & " " & T_Email_Addr & " Valido: " & T_Valido.ToString & " Contatto:" & U_Contatto.ContactID)

                                        If T_Valido Then
                                            If U_Contatto.ContactID = "" Then
                                                U_Contatto.Email = T_Email_Addr
                                                U_Contatto.FirstName = "[AUTO]"
                                                U_Contatto.LastName = "ACCOUNT " & T_account
                                                U_Contatto.AccountID = T_account
                                                U_Contatto.StatusCode = 2
                                                U_Contatto.GenderCode = 0


                                                Wc_Upd.Dati = JsonConvert.SerializeObject(U_Contatto, T_s)
                                                Wc_Upd.Metodo = "POST"
                                                Wc_Upd.URL = Wc_Upd.Server & Wc_Upd.Collection
                                                Wc_Upd.Elabora()
                                                If Not Wc_Upd.Err Then
                                                    Wc_Cont.Filtro = "Email eq '" & T_Email_Addr & "'"
                                                    Wc_Cont.Elabora()
                                                    Dati_Cont = JsonConvert.DeserializeObject(Of Dati_C4C_Contatto)(Wc_Cont.Result)
                                                    U_Contatto = Dati_Cont.d.results(0)
                                                End If
                                                Writelog("   Creato contatto:" & U_Contatto.ContactID)
                                            End If


                                            Dim R_ACC As result_account = leggi_Cust(U_Contatto.AccountID)
                                            If Not R_ACC Is Nothing Then

                                                Dim U_SR As New result_ServiceRequest
                                                U_SR.BuyerMainContactPartyID = U_Contatto.ContactID
                                                U_SR.BuyerPartyID = U_Contatto.AccountID

                                                Dim Idx_ST As Integer = 0
                                                Select Case R_ACC.AreaClienti_KUT
                                                    Case "ITI1"
                                                        Idx_ST = 24
                                                    Case "ITI2"
                                                        Idx_ST = 25
                                                    Case "ITI3"
                                                        Idx_ST = 11
                                                    Case "ITI4"
                                                        Idx_ST = 12
                                                    Case "ITI5"
                                                        Idx_ST = 13
                                                    Case "ITI6"
                                                        Idx_ST = 14
                                                End Select

                                                If R_ACC.ClasseMerceologica_KUT = "98" Or R_ACC.ClasseMerceologica_KUT = "99" Then
                                                    Idx_ST = 181
                                                End If

                                                U_SR.SalesTerritoryID = Idx_ST

                                                If Idx_ST <> 0 Then
                                                    Wc_UpdSr.Dati = JsonConvert.SerializeObject(U_SR, T_s)
                                                    Wc_UpdSr.Metodo = "PATCH"
                                                    Wc_UpdSr.URL = Wc_UpdSr.Server & Wc_UpdSr.Collection & "('" & L_SR.ObjectID & "')"
                                                    Wc_UpdSr.Elabora()
                                                    If Not Wc_UpdSr.Err Then
                                                        Writelog("   " & L_SR.ID & " elaborato ")
                                                    End If
                                                End If
                                            End If
                                        Else
                                            If l_Cont_Spec = "" Then
                                                Dim R_new As DataRow = T_Email.NewRow
                                                R_new("email") = T_Email_Addr
                                                R_new("conteggio") = 1
                                                T_Email.Rows.Add(R_new)
                                                T_Email.WriteXml("T_Email.xml")
                                            End If

                                            'If T_account = "" And T_account <> Id_NoLogo And U_Contatto.AccountID <> Id_NoLogo Then


                                            '    U_Contatto = New result_contatto
                                            '    U_Contatto.FirstName = "COMPLETARE"
                                            '    U_Contatto.LastName = "DA COMPLETARE"
                                            '    U_Contatto.AccountID = "1149037"
                                            '    U_Contatto.StatusCode = 2
                                            '    U_Contatto.GenderCode = 0
                                            '    U_Contatto.Email = T_Email_Addr


                                            '    Wc_Upd.Dati = JsonConvert.SerializeObject(U_Contatto, T_s)
                                            '    Wc_Upd.Metodo = "POST"
                                            '    Wc_Upd.URL = Wc_Upd.Server & Wc_Upd.Collection
                                            '    Wc_Upd.Elabora()
                                            '    If Not Wc_Upd.Err Then
                                            '        Wc_Cont.Filtro = "Email eq '" & T_Email_Addr & "'"
                                            '        Wc_Cont.Elabora()
                                            '        Dati_Cont = JsonConvert.DeserializeObject(Of Dati_C4C_Contatto)(Wc_Cont.Result)
                                            '        U_Contatto = Dati_Cont.d.results(0)
                                            '        Dim U_ESP As New result_EMailSenderParty
                                            '        U_ESP.PartyID = U_Contatto.ContactID
                                            '        U_ESP.EMailID = L_BTD.EMailID
                                            '        Wc_ESP.Dati = JsonConvert.SerializeObject(U_ESP, T_s)
                                            '        Wc_ESP.Metodo = "PATCH"
                                            '        Wc_ESP.URL = Wc_ESP.Server & Wc_ESP.Collection
                                            '        Wc_ESP.Elabora()

                                            '        Wc_Srt.Filtro = "ServiceRequestID eq '" & L_SR.ID & "'"
                                            '        Wc_Srt.Elabora()
                                            '        Dim Dati_Srt As New Dati_C4C_ServiceRequestTextCollection
                                            '        Dati_Srt = JsonConvert.DeserializeObject(Of Dati_C4C_ServiceRequestTextCollection)(Wc_Srt.Result)
                                            '        L_TxTEmail = "Da: " & T_Email_Addr & vbCrLf & "Soggetto: " & L_SR.Name & vbCrLf & vbCrLf
                                            '        For Each R_Srt As result_ServiceRequestTextCollection In Dati_Srt.d.results
                                            '            For T_i As Integer = 0 To R_Srt.Text.Split(vbLf).Count - 1
                                            '                Dim T_txt As String = R_Srt.Text.Split(vbLf)(T_i)
                                            '                T_txt = Replace(T_txt, vbCr, "")
                                            '                T_txt = Replace(T_txt, vbTab, "")
                                            '                If T_txt <> "" Then
                                            '                    L_TxTEmail &= T_txt & vbCrLf
                                            '                End If
                                            '            Next
                                            '        Next



                                            '        Dim Ins_Ctxt As New result_ContactTextCollection
                                            '        Wc_Ctxt.Collection = "ContactTextCollectionCollection"
                                            '        Ins_Ctxt.ContactID = U_Contatto.ContactID
                                            '        Ins_Ctxt.ParentObjectID = U_Contatto.ObjectID
                                            '        Ins_Ctxt.Text = L_TxTEmail
                                            '        Wc_Ctxt.Dati = JsonConvert.SerializeObject(Ins_Ctxt, T_s)
                                            '        Wc_Ctxt.Metodo = "POST"
                                            '        Wc_Ctxt.URL = Wc_Ctxt.Server & Wc_Ctxt.Collection
                                            '        Wc_Ctxt.Elabora()


                                            '        Stop

                                            '    End If
                                            'End If
                                        End If
                                    End If
                                Else
                                    For Each R_X As DataRow In T_Email.Select("email='" & Replace(T_Email_Addr, "'", "''") & "'")
                                        R_X("conteggio") = CInt(R_X("conteggio")) + 1
                                    Next
                                    T_Email.Select("email='" & Replace(T_Email_Addr, "'", "''") & "'")(0)("conteggio") += 1
                                End If
                            End If
                        Next

                    Next

                Next

                If Dati_ServiceRequest.d.results.Count < 1000 Then Exit Do
            Else
                Writelog(Wc.Result)
                Writelog(Wc.URL)
                Exit Do
            End If
            Wc.Skip += 1000
        Loop


        Dim l_DV As New DataView
        l_DV.Table = Tab_Domain
        l_DV.Sort = "dominio"

        For Each R_Dom As DataRowView In l_DV
            If R_Dom("nome").ToString = "" Then
                Dim R_Cust As result_account = leggi_Cust(R_Dom("account").ToString)
                If Not R_Cust Is Nothing Then
                    R_Dom("nome") = R_Cust.Name
                    R_Dom("citta") = R_Cust.City
                    R_Dom("tipo") = R_Cust.RoleCodeText
                    R_Dom("stato") = R_Cust.LifeCycleStatusCodeText
                    R_Dom("area") = R_Cust.AreaClienti_KUT
                    R_Dom("piva") = R_Cust.PartitaIva_KUT
                    R_Dom("codsap") = R_Cust.ExternalID
                    Writelog(R_Dom("dominio").ToString & " " & R_Dom("account").ToString & " " & R_Dom("nome").ToString & R_Cust.AreaClienti_KUTText)
                End If
            End If
        Next

        Tab_Domain.WriteXml("Tab_Domain.xml")

        If Dir("T_Email.xml") <> "" Then
            If Dir("T_Email_old.xml") <> "" Then
                Kill("T_Email_old.xml")
            End If
            FileCopy("T_Email.xml", "T_Email_old.xml")
            Kill("T_Email.xml")
        End If


        If Dir("Tab_Domain.xml") <> "" Then
            If Dir("Tab_Domain_old.xml") <> "" Then
                Kill("Tab_Domain_old.xml")
            End If

            FileCopy("Tab_Domain.xml", "Tab_Domain_old.xml")
            Kill("Tab_Domain.xml")
        End If





        Writelog("Elaborazione Finita")
        Writelog("Tempo Esecuzione: " & DateDiff(DateInterval.Second, T_start, Now).ToString & " sec")



    End Sub

    Function leggi_Cust(AccountID As String) As result_account
        If List_Cust.ContainsKey(AccountID) Then
            Return List_Cust(AccountID)
            Exit Function
        Else
            Wc_Cust.Filtro = "AccountID eq '" & AccountID & "'"
            Wc_Cust.Elabora()
            If Not Wc_Cust.Err Then
                Dati_Cust = JsonConvert.DeserializeObject(Of Dati_C4C_Accont)(Wc_Cust.Result)
                For Each R_Cust As result_account In Dati_Cust.d.results
                    List_Cust.Add(AccountID, R_Cust)
                    Return R_Cust
                    Exit Function
                Next
            End If
        End If
        Return Nothing
    End Function


End Module
