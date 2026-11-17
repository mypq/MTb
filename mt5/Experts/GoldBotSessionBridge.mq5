#property strict
#property version   "1.10"
#property description "Exports broker sessions and append-only raw clock evidence."

input string InpSymbol = "";
input int InpRefreshSeconds = 60;
input int InpMaxSessionsPerDay = 32;

#define SESSION_FOLDER       "GoldBot"
#define SESSION_FILE         "sessions.json"
#define SESSION_TEMP         "sessions.json.tmp"
#define SESSION_SCHEMA_VERSION 1

#define CLOCK_HISTORY_FILE   "clock_history.csv"
#define CLOCK_LOCK_FILE     "clock_history.writer.lock"
#define CLOCK_SCHEMA_VERSION 1

string g_symbol = "";
string g_run_id = "";
long g_sample_sequence = 0;
int g_clock_lock_handle = INVALID_HANDLE;

//+------------------------------------------------------------------+
//| JSON escaping                                                    |
//+------------------------------------------------------------------+
string JsonEscape(const string value)
{
   string escaped = "";

   for(int i = 0; i < StringLen(value); i++)
   {
      ushort ch = (ushort)StringGetCharacter(value, i);

      if(ch == 34)
         escaped += "\\\"";
      else if(ch == 92)
         escaped += "\\\\";
      else if(ch == 8)
         escaped += "\\b";
      else if(ch == 9)
         escaped += "\\t";
      else if(ch == 10)
         escaped += "\\n";
      else if(ch == 12)
         escaped += "\\f";
      else if(ch == 13)
         escaped += "\\r";
      else if(ch < 32)
         escaped += StringFormat("\\u%04X", (int)ch);
      else
         escaped += ShortToString(ch);
   }

   return escaped;
}

//+------------------------------------------------------------------+
//| Sanitize text fields used in CSV                                 |
//+------------------------------------------------------------------+
string CleanCsvField(string value)
{
   StringReplace(value, ";", "_");
   StringReplace(value, "\r", " ");
   StringReplace(value, "\n", " ");
   StringReplace(value, "\"", "'");

   return value;
}

//+------------------------------------------------------------------+
//| Acquire exclusive writer lock                                    |
//+------------------------------------------------------------------+
bool AcquireClockHistoryLock()
{
   FolderCreate(SESSION_FOLDER, FILE_COMMON);

   ResetLastError();

   g_clock_lock_handle = FileOpen(
      SESSION_FOLDER + "\\" + CLOCK_LOCK_FILE,
      FILE_WRITE | FILE_COMMON
   );

   if(g_clock_lock_handle == INVALID_HANDLE)
   {
      PrintFormat(
         "Clock history writer lock unavailable. error=%d",
         GetLastError()
      );
      return false;
   }

   return true;
}

//+------------------------------------------------------------------+
//| Release writer lock                                              |
//+------------------------------------------------------------------+
void ReleaseClockHistoryLock()
{
   if(g_clock_lock_handle != INVALID_HANDLE)
   {
      FileClose(g_clock_lock_handle);
      g_clock_lock_handle = INVALID_HANDLE;
   }
}

//+------------------------------------------------------------------+
//| Expected CSV header                                              |
//+------------------------------------------------------------------+
string ClockHistoryHeader()
{
   return
      "schema_version;run_id;sample_sequence;monotonic_ms;"
      "time_gmt_epoch;time_trade_server_epoch;"
      "time_current_quote_epoch;time_local_epoch;"
      "server_utc_offset_seconds;clock_values_valid;"
      "account_login;broker_server;symbol;"
      "terminal_build;terminal_connected";
}

//+------------------------------------------------------------------+
//| Validate existing CSV header                                     |
//+------------------------------------------------------------------+
bool ValidateClockHistoryHeader()
{
   string path = SESSION_FOLDER + "\\" + CLOCK_HISTORY_FILE;

   if(!FileIsExist(path, FILE_COMMON))
      return true;

   ResetLastError();

   int handle = FileOpen(
      path,
      FILE_READ | FILE_TXT | FILE_ANSI | FILE_COMMON
   );

   if(handle == INVALID_HANDLE)
   {
      PrintFormat(
         "Cannot read clock history header. error=%d",
         GetLastError()
      );
      return false;
   }

   if(FileSize(handle) == 0)
   {
      FileClose(handle);
      return true;
   }

   string header = FileReadString(handle);
   FileClose(handle);

   if(header != ClockHistoryHeader())
   {
      Print(
         "Clock history header mismatch. "
         "Existing file was preserved; append refused."
      );
      return false;
   }

   return true;
}

//+------------------------------------------------------------------+
//| Reject incomplete final CSV record                               |
//+------------------------------------------------------------------+
bool ValidateClockHistoryTail()
{
   string path = SESSION_FOLDER + "\\" + CLOCK_HISTORY_FILE;

   if(!FileIsExist(path, FILE_COMMON))
      return true;

   ResetLastError();

   int handle = FileOpen(
      path,
      FILE_READ | FILE_BIN | FILE_COMMON
   );

   if(handle == INVALID_HANDLE)
   {
      PrintFormat(
         "Cannot inspect clock history tail. error=%d",
         GetLastError()
      );
      return false;
   }

   long size = FileSize(handle);

   if(size == 0)
   {
      FileClose(handle);
      return true;
   }

   if(!FileSeek(handle, -1, SEEK_END))
   {
      Print("Cannot seek to clock history final byte.");
      FileClose(handle);
      return false;
   }

   uchar last_byte = (uchar)FileReadInteger(handle, CHAR_VALUE);
   FileClose(handle);

   if(last_byte != 10)
   {
      Print(
         "Clock history has an incomplete final record. "
         "No append was performed; existing evidence was preserved."
      );
      return false;
   }

   return true;
}

//+------------------------------------------------------------------+
//| Append one raw clock sample                                      |
//+------------------------------------------------------------------+
bool AppendClockHistorySample()
{
   if(g_clock_lock_handle == INVALID_HANDLE)
   {
      Print("Clock history lock is not held.");
      return false;
   }

   if(!ValidateClockHistoryHeader())
      return false;

   if(!ValidateClockHistoryTail())
      return false;

   datetime gmt_now = TimeGMT();
   datetime server_now = TimeTradeServer();
   datetime quote_time = TimeCurrent();
   datetime local_now = TimeLocal();

   bool clock_values_valid =
      (gmt_now > 0 && server_now > 0);

   long offset_seconds = 0;

   if(clock_values_valid)
   {
      offset_seconds =
         (long)server_now - (long)gmt_now;
   }

   long account_login =
      AccountInfoInteger(ACCOUNT_LOGIN);

   string broker_server =
      CleanCsvField(AccountInfoString(ACCOUNT_SERVER));

   string symbol =
      CleanCsvField(g_symbol);

   int terminal_build =
      (int)TerminalInfoInteger(TERMINAL_BUILD);

   bool terminal_connected =
      (bool)TerminalInfoInteger(TERMINAL_CONNECTED);

   if(account_login <= 0 ||
      StringLen(broker_server) == 0 ||
      StringLen(symbol) == 0 ||
      terminal_build <= 0)
   {
      Print(
         "Clock sample identity is incomplete. "
         "Sample rejected."
      );
      return false;
   }

   string path =
      SESSION_FOLDER + "\\" + CLOCK_HISTORY_FILE;

   ResetLastError();

   int handle = FileOpen(
      path,
      FILE_READ | FILE_WRITE |
      FILE_CSV | FILE_ANSI | FILE_COMMON,
      ';'
   );

   if(handle == INVALID_HANDLE)
   {
      PrintFormat(
         "Cannot open clock history for append. error=%d",
         GetLastError()
      );
      return false;
   }

   if(FileSize(handle) == 0)
   {
      uint header_written = FileWrite(
         handle,
         "schema_version",
         "run_id",
         "sample_sequence",
         "monotonic_ms",
         "time_gmt_epoch",
         "time_trade_server_epoch",
         "time_current_quote_epoch",
         "time_local_epoch",
         "server_utc_offset_seconds",
         "clock_values_valid",
         "account_login",
         "broker_server",
         "symbol",
         "terminal_build",
         "terminal_connected"
      );

      if(header_written == 0)
      {
         Print("Clock history header write failed.");
         FileClose(handle);
         return false;
      }

      FileFlush(handle);
   }

   if(!FileSeek(handle, 0, SEEK_END))
   {
      Print("Clock history seek-to-end failed.");
      FileClose(handle);
      return false;
   }

   long next_sequence = g_sample_sequence + 1;

   uint written = FileWrite(
      handle,
      CLOCK_SCHEMA_VERSION,
      g_run_id,
      next_sequence,
      (long)GetTickCount(),
      (long)gmt_now,
      (long)server_now,
      (long)quote_time,
      (long)local_now,
      offset_seconds,
      clock_values_valid,
      account_login,
      broker_server,
      symbol,
      terminal_build,
      terminal_connected
   );

   if(written == 0)
   {
      Print("Clock history sample write failed.");
      FileClose(handle);
      return false;
   }

   FileFlush(handle);
   FileClose(handle);

   g_sample_sequence = next_sequence;

   PrintFormat(
      "Clock sample appended: run=%s sequence=%I64d valid=%s",
      g_run_id,
      g_sample_sequence,
      clock_values_valid ? "true" : "false"
   );

   return true;
}

//+------------------------------------------------------------------+
//| Append trading or quote sessions (Updated)                       |
//+------------------------------------------------------------------+
bool AppendSessionArray(
   string &json,
   const int day,
   const bool trading_sessions
)
{
   json += "[";

   int written_sessions = 0;

   for(int index = 0; index < InpMaxSessionsPerDay; index++)
   {
      datetime session_from = 0;
      datetime session_to = 0;
      bool found = false;

      if(trading_sessions)
      {
         found = SymbolInfoSessionTrade(
            g_symbol,
            (ENUM_DAY_OF_WEEK)day,
            (uint)index,
            session_from,
            session_to
         );
      }
      else
      {
         found = SymbolInfoSessionQuote(
            g_symbol,
            (ENUM_DAY_OF_WEEK)day,
            (uint)index,
            session_from,
            session_to
         );
      }

      if(!found)
         break;

      long from_seconds = (long)session_from;
      long to_seconds = (long)session_to;

      if(from_seconds < 0 || from_seconds > 86400 ||
         to_seconds < 0 || to_seconds > 86400 ||
         from_seconds == to_seconds)
      {
         PrintFormat(
            "Invalid session values: symbol=%s day=%d index=%d",
            g_symbol,
            day,
            index
         );
         return false;
      }

      if(written_sessions > 0)
         json += ",";

      json += StringFormat(
         "{\"from_seconds\":%I64d,\"to_seconds\":%I64d}",
         from_seconds,
         to_seconds
      );

      written_sessions++;
   }

   datetime extra_from = 0;
   datetime extra_to = 0;
   bool has_extra = false;

   if(trading_sessions)
   {
      has_extra = SymbolInfoSessionTrade(
         g_symbol,
         (ENUM_DAY_OF_WEEK)day,
         (uint)InpMaxSessionsPerDay,
         extra_from,
         extra_to
      );
   }
   else
   {
      has_extra = SymbolInfoSessionQuote(
         g_symbol,
         (ENUM_DAY_OF_WEEK)day,
         (uint)InpMaxSessionsPerDay,
         extra_from,
         extra_to
      );
   }

   if(has_extra)
   {
      PrintFormat(
         "Session limit exceeded: symbol=%s day=%d",
         g_symbol,
         day
      );
      return false;
   }

   json += "]";
   return true;
}

//+------------------------------------------------------------------+
//| Build weekly schedule JSON                                       |
//+------------------------------------------------------------------+
bool BuildScheduleJson(string &json)
{
   datetime server_now = TimeTradeServer();
   datetime utc_now = TimeGMT();

   if(server_now <= 0 || utc_now <= 0)
   {
      Print("Broker server time or UTC time is unavailable.");
      return false;
   }

   long server_utc_offset =
      (long)server_now - (long)utc_now;

   json = StringFormat(
      "{\"schema_version\":%d,\"status\":\"ok\",\"symbol\":\"%s\","
      "\"generated_at_utc_epoch\":%I64d,"
      "\"server_utc_offset_seconds\":%I64d,"
      "\"schedule_kind\":\"weekly_recurring\",\"days\":[",
      SESSION_SCHEMA_VERSION,
      JsonEscape(g_symbol),
      (long)utc_now,
      server_utc_offset
   );

   for(int day = 0; day < 7; day++)
   {
      if(day > 0)
         json += ",";

      json += StringFormat(
         "{\"day_of_week\":%d,\"trade\":",
         day
      );

      if(!AppendSessionArray(json, day, true))
         return false;

      json += ",\"quote\":";

      if(!AppendSessionArray(json, day, false))
         return false;

      json += "}";
   }

   json += "]}";
   return true;
}

//+------------------------------------------------------------------+
//| Publish schedule atomically                                      |
//+------------------------------------------------------------------+
bool PublishSchedule()
{
   string json = "";

   if(!BuildScheduleJson(json))
      return false;

   FolderCreate(SESSION_FOLDER, FILE_COMMON);

   string temp_path =
      SESSION_FOLDER + "\\" + SESSION_TEMP;

   string final_path =
      SESSION_FOLDER + "\\" + SESSION_FILE;

   ResetLastError();

   int handle = FileOpen(
      temp_path,
      FILE_WRITE | FILE_TXT | FILE_ANSI | FILE_COMMON
   );

   if(handle == INVALID_HANDLE)
   {
      PrintFormat(
         "Cannot open temporary schedule file. error=%d",
         GetLastError()
      );
      return false;
   }

   uint written = FileWriteString(handle, json);

   FileFlush(handle);
   FileClose(handle);

   if(written != (uint)StringLen(json))
   {
      Print(
         "Incomplete schedule write. "
         "Existing published file was preserved."
      );
      FileDelete(temp_path, FILE_COMMON);
      return false;
   }

   ResetLastError();

   if(!FileMove(
      temp_path,
      FILE_COMMON,
      final_path,
      FILE_COMMON | FILE_REWRITE
   ))
   {
      PrintFormat(
         "Schedule publication failed. error=%d",
         GetLastError()
      );

      FileDelete(temp_path, FILE_COMMON);
      return false;
   }

   PrintFormat(
      "Broker sessions published for %s.",
      g_symbol
   );

   return true;
}

//+------------------------------------------------------------------+
//| Initialization                                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   if(InpRefreshSeconds < 10 || InpRefreshSeconds > 3600)
   {
      Print("Refresh interval must be between 10 and 3600 seconds.");
      return INIT_PARAMETERS_INCORRECT;
   }

   if(InpMaxSessionsPerDay < 1 || InpMaxSessionsPerDay > 32)
   {
      Print("Maximum sessions must be between 1 and 32.");
      return INIT_PARAMETERS_INCORRECT;
   }

   g_symbol = InpSymbol;

   if(StringLen(g_symbol) == 0)
      g_symbol = _Symbol;

   if(!SymbolSelect(g_symbol, true))
   {
      PrintFormat("Cannot select symbol %s.", g_symbol);
      return INIT_FAILED;
   }

   if(!AcquireClockHistoryLock())
      return INIT_FAILED;

   g_run_id = StringFormat(
      "%I64d_%u",
      (long)TimeLocal(),
      GetTickCount()
   );

   g_sample_sequence = 0;

   if(!PublishSchedule())
   {
      Print("Initial broker schedule export failed.");
      ReleaseClockHistoryLock();
      return INIT_FAILED;
   }

   if(!AppendClockHistorySample())
   {
      Print("Initial clock history sample failed.");
      ReleaseClockHistoryLock();
      return INIT_FAILED;
   }

   if(!EventSetTimer(InpRefreshSeconds))
   {
      PrintFormat(
         "Timer initialization failed. error=%d",
         GetLastError()
      );

      ReleaseClockHistoryLock();
      return INIT_FAILED;
   }

   PrintFormat(
      "GoldBot session and clock bridge started for %s. run_id=%s",
      g_symbol,
      g_run_id
   );

   return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| Timer                                                            |
//+------------------------------------------------------------------+
void OnTimer()
{
   if(!PublishSchedule())
      Print("Schedule refresh failed; previous file remains untouched.");

   if(!AppendClockHistorySample())
      Print("Clock history sample failed; existing history was preserved.");
}

//+------------------------------------------------------------------+
//| Deinitialization                                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();

   ReleaseClockHistoryLock();

   PrintFormat(
      "GoldBot session and clock bridge stopped. reason=%d",
      reason
   );
}
