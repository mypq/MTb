#property strict
#property version   "1.00"
#property description "Exports the connected broker's weekly trading and quote sessions."

input string InpSymbol = "";
input int InpRefreshSeconds = 60;
input int InpMaxSessionsPerDay = 32;

#define SESSION_FOLDER "GoldBot"
#define SESSION_FILE   "sessions.json"
#define SESSION_TEMP   "sessions.json.tmp"
#define SESSION_SCHEMA_VERSION 1

string g_symbol = "";

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

bool AppendSessionArray(
   string &json,
   const int day,
   const bool trading_sessions
)
{
   json += "[";

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
         return true;

      if(index > 0)
         json += ",";

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

      json += StringFormat(
         "{\"from_seconds\":%I64d,\"to_seconds\":%I64d}",
         from_seconds,
         to_seconds
      );
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
      Print("Incomplete schedule write. Existing published file was preserved.");
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

   if(!PublishSchedule())
   {
      Print("Initial broker schedule export failed.");
      return INIT_FAILED;
   }

   if(!EventSetTimer(InpRefreshSeconds))
   {
      PrintFormat(
         "Timer initialization failed. error=%d",
         GetLastError()
      );
      return INIT_FAILED;
   }

   PrintFormat(
      "GoldBot session bridge started for %s.",
      g_symbol
   );

   return INIT_SUCCEEDED;
}

void OnTimer()
{
   if(!PublishSchedule())
      Print("Schedule refresh failed; previous file remains untouched.");
}

void OnDeinit(const int reason)
{
   EventKillTimer();

   PrintFormat(
      "GoldBot session bridge stopped. reason=%d",
      reason
   );
}
