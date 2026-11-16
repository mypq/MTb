
#property strict
#property version   "1.00"
#property description "Read-only MT5 broker clock diagnostic probe."

input string InpSymbol = "";
input int InpDurationSeconds = 900;

#define PROBE_FOLDER "GoldBot"
#define PROBE_FILE   "clock_probe.csv"

string g_symbol = "";
int g_file = INVALID_HANDLE;
uint g_started_ms = 0;
long g_sample_index = 0;

void CloseProbeFile()
{
   if(g_file != INVALID_HANDLE)
   {
      FileFlush(g_file);
      FileClose(g_file);
      g_file = INVALID_HANDLE;
   }
}

int OnInit()
{
   if(InpDurationSeconds < 60)
   {
      Print("ERROR: Duration must be at least 60 seconds.");
      return INIT_PARAMETERS_INCORRECT;
   }

   g_symbol = InpSymbol;

   if(StringLen(g_symbol) == 0)
      g_symbol = _Symbol;

   if(!SymbolSelect(g_symbol, true))
   {
      PrintFormat(
         "ERROR: Cannot select symbol %s. Error=%d",
         g_symbol,
         GetLastError()
      );
      return INIT_FAILED;
   }

   FolderCreate(PROBE_FOLDER, FILE_COMMON);

   string path = PROBE_FOLDER + "\\" + PROBE_FILE;

   ResetLastError();

   g_file = FileOpen(
      path,
      FILE_WRITE | FILE_CSV | FILE_ANSI | FILE_COMMON,
      ','
   );

   if(g_file == INVALID_HANDLE)
   {
      PrintFormat(
         "ERROR: Cannot open probe CSV. Error=%d",
         GetLastError()
      );
      return INIT_FAILED;
   }

   FileWrite(
      g_file,
      "sample_index",
      "symbol",
      "broker_server",
      "event_time_current_epoch",
      "symbol_tick_time_msc",
      "symbol_tick_time_epoch",
      "time_trade_server_epoch",
      "time_gmt_epoch",
      "time_local_epoch",
      "m1_current_open_epoch",
      "m1_closed_open_epoch",
      "m5_current_open_epoch",
      "m5_closed_open_epoch",
      "tick_bid",
      "tick_ask",
      "tick_volume",
      "get_tick_count_ms"
   );

   FileFlush(g_file);

   g_started_ms = GetTickCount();

   if(!EventSetTimer(1))
   {
      PrintFormat(
         "ERROR: Cannot start duration timer. Error=%d",
         GetLastError()
      );
      CloseProbeFile();
      return INIT_FAILED;
   }

   Print("Read-only clock probe started.");
   PrintFormat("Symbol: %s", g_symbol);
   PrintFormat("Duration: %d seconds", InpDurationSeconds);
   PrintFormat("Output: Common Files\\%s", path);

   return INIT_SUCCEEDED;
}

void OnTick()
{
   if(g_file == INVALID_HANDLE)
      return;

   MqlTick tick;

   if(!SymbolInfoTick(g_symbol, tick))
   {
      PrintFormat(
         "WARNING: SymbolInfoTick failed. Error=%d",
         GetLastError()
      );
      return;
   }

   datetime event_time = TimeCurrent();
   datetime trade_server_time = TimeTradeServer();
   datetime gmt_time = TimeGMT();
   datetime local_time = TimeLocal();

   datetime m1_current = iTime(g_symbol, PERIOD_M1, 0);
   datetime m1_closed = iTime(g_symbol, PERIOD_M1, 1);

   datetime m5_current = iTime(g_symbol, PERIOD_M5, 0);
   datetime m5_closed = iTime(g_symbol, PERIOD_M5, 1);

   g_sample_index++;

   FileWrite(
      g_file,
      g_sample_index,
      g_symbol,
      AccountInfoString(ACCOUNT_SERVER),
      (long)event_time,
      (long)tick.time_msc,
      (double)tick.time_msc / 1000.0,
      (long)trade_server_time,
      (long)gmt_time,
      (long)local_time,
      (long)m1_current,
      (long)m1_closed,
      (long)m5_current,
      (long)m5_closed,
      tick.bid,
      tick.ask,
      (long)tick.volume,
      (long)GetTickCount()
   );

   FileFlush(g_file);
}

void OnTimer()
{
   uint elapsed_ms = (uint)(GetTickCount() - g_started_ms);

   if(elapsed_ms >= (uint)(InpDurationSeconds * 1000))
   {
      PrintFormat(
         "Clock probe finished. Samples=%I64d",
         g_sample_index
      );

      CloseProbeFile();
      EventKillTimer();
      ExpertRemove();
   }
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   CloseProbeFile();

   PrintFormat(
      "Clock probe stopped. Samples=%I64d Reason=%d",
      g_sample_index,
      reason
   );
}
