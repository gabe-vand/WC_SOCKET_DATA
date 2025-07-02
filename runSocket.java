import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/*
 * Add indexing for each web socket so they have identifiers, and track which tickers go where.
 * On occasion, ping each websocket for a response. 
 * If no response or checksum error, ws.abort() and then re-open. remove old websocket from arraylist, add new one in place with same ID#.
 */


public class runSocket
{
	public static ArrayList<BaseSocket> sockets = new ArrayList<BaseSocket>(); //list of all of our book sockets
	public static HashMap<Integer, List<String>> socketInfo = new HashMap<Integer, List<String>>();
	
	public static HashMap<String, Double> lastClose = new HashMap<String, Double>();
	public static ConcurrentHashMap<String, Double> lastMedianSpread1 = new ConcurrentHashMap<String, Double>();
	public static ConcurrentHashMap<String, Double> lastMedianSpread10 = new ConcurrentHashMap<String, Double>();
	public static ConcurrentHashMap<String, Double> lastMedianSpread25 = new ConcurrentHashMap<String, Double>();
	public static ConcurrentHashMap<String, Double> lastMedianSpread50 = new ConcurrentHashMap<String, Double>();

	public static ConcurrentHashMap<String, ArrayList<Double>> medianSpreads1 = new ConcurrentHashMap<String, ArrayList<Double>>();
	public static ConcurrentHashMap<String, ArrayList<Double>> medianSpreads10 = new ConcurrentHashMap<String, ArrayList<Double>>();
	public static ConcurrentHashMap<String, ArrayList<Double>> medianSpreads25 = new ConcurrentHashMap<String, ArrayList<Double>>();
	public static ConcurrentHashMap<String, ArrayList<Double>> medianSpreads50 = new ConcurrentHashMap<String, ArrayList<Double>>();
	public static ConcurrentHashMap<String, ArrayList<String>> trades = new ConcurrentHashMap<String, ArrayList<String>>();
	public static ConcurrentHashMap<String, OrderBook> orderBooks = new ConcurrentHashMap<String, OrderBook>();
		
	public static void main(String[] args)
	{
			
		List<String> tickers = Utils.getTickers();
		tickers.remove("SSV/USD"); tickers.remove("K/USD"); tickers.remove("EUL/USD"); tickers.remove("LSETH/USD"); //bad tickers - consistenly cause checksum issues for unknown reason
		System.out.println("Running for " + tickers.size() + " tickers.");
		System.out.println(tickers);
		
		tickers.forEach(tick -> {
			try {
				tick = tick.replace("/", "");
				String outputFile = "C:\\Trading\\Data\\Crypto\\KrakenSocket\\" + tick + ".csv";
				
				 try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, false))) {
					bw.write("timestamp,open,high,low,close,volume,mSpread1,mSpread10,mSpread25,mSpread50");
		            bw.newLine();
	
		        } catch (IOException e) {
		            e.printStackTrace();
		        }
			} catch (Exception e) { e.printStackTrace(); }
		});
			
		
		String subscriptionTrades = Utils.getSubMessage("trades"); //for streaming trades data
	    BaseSocket socketTrades = stream(true, subscriptionTrades);
	    sockets.add(socketTrades);
	    socketInfo.put(0, List.of("TICKERS"));	    
	    
	    int groupSize = 9; //controls how many market subscriptions are sent to one websocket
	    
	    int socketsNeeded = (tickers.size() + groupSize - 1) / groupSize; // for grouping markets up and asisgning them to sockets to avoid truncation issue

	    for (int i = 1; i < socketsNeeded + 1; i++) {
	        int start = i * groupSize - groupSize;
	        int end   = Math.min(start + groupSize, tickers.size());
	        List<String> group = tickers.subList(start, end);

	        String subscriptionBook = buildBookSubscription(group);
	        BaseSocket socketBook   = stream(true, subscriptionBook);
	        socketInfo.put(i, group);
	        sockets.add(socketBook);
	    }
	    
		System.out.println(sockets.size() + " sockets, groups of " + groupSize);
		//System.out.println(socketInfo);

	    ScheduledExecutorService schedulerMin = Executors.newSingleThreadScheduledExecutor();
	    ExecutorService workersMin            = Executors.newFixedThreadPool(2);
	    ScheduledExecutorService scheduler5   = Executors.newSingleThreadScheduledExecutor();
	    ExecutorService workers5              = Executors.newFixedThreadPool(2);

	    Runnable jobMin = () -> { //once per minute, get trade data in raw form (hash map: market -> raw json of trades). parse and record using processData()
	    	try
	    	{
	            workersMin.submit(() ->
	            {
	            	trades = socketTrades.getTrades();
	            	tickers.forEach(ticker -> trades.computeIfAbsent(ticker, k -> new ArrayList<>()));
	                processData(trades);
	                socketTrades.clearTrades();
	                trades.clear();
	                clearSpreads();
	            });
	    	} catch (Exception e) { e.printStackTrace(); }
	    };
	    
	    Runnable job5 = () -> { //every five seconds, get spread data from order books.
	    	try
	    	{
	            workers5.submit(() -> 
	            {
	            	sockets.forEach(socket -> orderBooks.putAll(socket.getBooks())); //populate orderBooks with the accumulation of all data from all of our book sockets
	                processBooks(orderBooks);
	                orderBooks.clear();
	            });
	    	} catch (Exception e) { e.printStackTrace(); }
	    };
	    

		ScheduledExecutorService socketChecker = Executors.newSingleThreadScheduledExecutor(); // reconnecting offline sockets loop
		ExecutorService workerSocket   = Executors.newFixedThreadPool(1);
		Runnable socketCheck = () -> workerSocket.submit(() -> {
			try { pingAndReconnectSockets(); } catch (Exception e) { e.printStackTrace(); }
		});
		//if tarade shuts down amke sure we send right message
		ScheduledExecutorService checksumChecker = Executors.newSingleThreadScheduledExecutor(); // resetting if chacksums are bad
		ExecutorService checksumWorker   = Executors.newFixedThreadPool(1);
		Runnable checksumCheck = () -> checksumWorker.submit(() -> {
			List<String> badSums = BaseSocket.getBadChecksums();
			if (!badSums.isEmpty()) resetSocketFromMarket(badSums);
			BaseSocket.clearBadChecksums();

		});
	    
	    long now   = System.currentTimeMillis();
	    long delay = 60_000 - (now % 60_000); //run at the start of new unix time minute
	    // schedule first run at the top‐of‐the‐minute, then every 60 s
	    schedulerMin.scheduleAtFixedRate(jobMin, delay, 60_000, TimeUnit.MILLISECONDS);
	    scheduler5.scheduleAtFixedRate(job5, delay - 1_500, 5_000, TimeUnit.MILLISECONDS);
	    socketChecker.scheduleAtFixedRate(socketCheck, 7_500, 15_000, TimeUnit.MILLISECONDS);
	    checksumChecker.scheduleAtFixedRate(checksumCheck, 5_000, 5_000, TimeUnit.MILLISECONDS);

	}
	
	public static void resetSocketFromMarket(List<String> badChecksums) {
		System.out.println("BAD SUMS: " + badChecksums);
		List<Integer> resetIndicies = new ArrayList<>();

		for (String badMarket : badChecksums)
		{
		    int index = -1;
		    
		    for (Integer socketNum : socketInfo.keySet())
		    {
		    	List<String> markets = socketInfo.get(socketNum);
		    	if (markets.contains(badMarket)) { index = socketNum; break; }
		    } 
		    
		    if (!resetIndicies.contains(index) && index != -1)
		    {
	            System.out.println("reconnecting socket " + index + " with tickers " + socketInfo.get(index));
		    	try { sockets.get(index).ws.abort(); } catch (Exception e) { } 
		        // build & open new
		        String subMsg = buildBookSubscription(socketInfo.get(index));
		        BaseSocket replacement = stream(true, subMsg);
		        // swap into the list
		        sockets.set(index, replacement);
		    }
		    
		    resetIndicies.add(index);
		}
	}
	
	public static void pingAndReconnectSockets() {
	    BaseSocket.clearPings();
	    int n = sockets.size();

	    for (int i = 0; i < n; i++) {
	        BaseSocket sock = sockets.get(i);
	        if (sock.ws != null) {
	            String msg = String.format("{\"method\":\"ping\",\"req_id\":%d}", i);
	            sock.ws.sendText(msg, true);
	        }
	    }

	    try { Thread.sleep(1_000); } catch (InterruptedException ignore) {}

	    Set<Integer> seen = new HashSet<>(BaseSocket.getPings());
	    //System.out.println(seen.size() + " pongs seen this round: " + seen);
	    
	    if (seen.size() == n) return;

	    for (int i = 0; i < n; i++) {
	        if (!seen.contains(i)) {
	            System.out.println("reconnecting offline socket " + i);
	            try { sockets.get(i).ws.abort(); } catch (Exception e) { }
	            // build + open new
	            String subMsg;
	            if (i == 0) subMsg = Utils.getSubMessage("trades");
	            else subMsg = buildBookSubscription(socketInfo.get(i));
	            BaseSocket replacement = stream(true, subMsg);
	            // swap into the list
	            sockets.set(i, replacement);
	        }
	    }
	}
	
	private static String buildBookSubscription(List<String> pairs) {
	    // join symbols as: "BTC/USD","ETH/USD",...
	    String symbols = pairs.stream().map(p -> "\"" + p + "\"").collect(Collectors.joining(",")); // string magic
	    
	    String template = """
	        {
	          "method": "subscribe",
	          "params": {
	            "channel": "book",
	            "symbol": [%s],
	            "depth": 10,
	            "snapshot": true
	          }
	        }
	        """;
	    
	    return String.format(template, symbols);
	}
	
	static void clearSpreads()
	{
		medianSpreads1.clear();
        medianSpreads10.clear();
        medianSpreads25.clear();
        medianSpreads50.clear();

	}
	
	public static BaseSocket stream(boolean filterHeartbeat, String subscription) {

		BaseSocket publicSocket = new PublicSocket(subscription, filterHeartbeat, "wss://ws.kraken.com/v2");

		Thread wsThread = new Thread(publicSocket::OpenAndStreamWebSocketSubscription, "kraken-ws");
		wsThread.start();
		
		return publicSocket;
	}
	
	public static void processBooks(ConcurrentHashMap<String, OrderBook> orderBooks)
	{
		String ticker = "";
		OrderBook orderBook;
		HashMap<String, String> bids;
		HashMap<String, String> asks;
		
		for (Map.Entry<String,OrderBook> entry : orderBooks.entrySet()) // px, qty
		{			
			ticker    = entry.getKey();
			orderBook = entry.getValue();
			bids      = orderBook.getBids();
			asks      = orderBook.getAsks();
			
			TreeMap<Double,Double> bidsTree = new TreeMap<>(Comparator.reverseOrder()); //highest first
			bids.forEach((k,v) -> bidsTree.put(Double.parseDouble(k), Double.parseDouble(v)) );

			TreeMap<Double,Double> asksTree = new TreeMap<>(); //lowest first
			asks.forEach((k,v) -> asksTree.put(Double.parseDouble(k), Double.parseDouble(v)) );
			
			double iMedianSpread1 = getSpread(bidsTree, asksTree, 1);
			medianSpreads1.computeIfAbsent(ticker, k -> new ArrayList<Double>()).add(iMedianSpread1);
			
			double iMedianSpread10 = getSpread(bidsTree, asksTree, 10_000);
			medianSpreads10.computeIfAbsent(ticker, k -> new ArrayList<Double>()).add(iMedianSpread10);
			
			double iMedianSpread25 = getSpread(bidsTree, asksTree, 25_000);
			medianSpreads25.computeIfAbsent(ticker, k -> new ArrayList<Double>()).add(iMedianSpread25);
			
			double iMedianSpread50 = getSpread(bidsTree, asksTree, 50_000);
			medianSpreads50.computeIfAbsent(ticker, k -> new ArrayList<Double>()).add(iMedianSpread50);
			
		}
	}
	
	private static double getSpread(TreeMap<Double, Double> bids, TreeMap<Double, Double> asks, int threshold)
	{		
		double bidPx = getWeightedPx(bids, threshold);
		double askPx = getWeightedPx(asks, threshold);
		
		return round((askPx - bidPx) / (0.5 * (askPx + bidPx)), 8);
	}
	
	
	public static double getWeightedPx(TreeMap<Double, Double> prices, int threshold)
	{
		double prevDollarSum = 0;
		double prevSquaredSum = 0;

		for (Map.Entry<Double,Double> e : prices.entrySet()) {
		    double price = e.getKey();
		    double qty   = e.getValue();
		    double dollar = price * qty;
		    double squared = price*price*qty;

		    if (prevDollarSum + dollar >= threshold) {
		        double neededDollar = threshold - prevDollarSum;    
		        double usedQty      = neededDollar / price;
		        double usedSquared  = price*price*usedQty;          

		        double weightedSum  = prevSquaredSum + usedSquared;
		        return weightedSum / threshold;
		    }

		    prevDollarSum   += dollar;
		    prevSquaredSum  += squared;
		}

		
		return prevSquaredSum / prevDollarSum;
	}
	
	
	public static void processData(ConcurrentHashMap<String, ArrayList<String>> allTrades) 
	{
		//allTrades.forEach((t, trades) -> { System.out.println("Ticker: " + t + ", Trades (raw): " + trades); });
		HashMap<String, Integer> tradeAmounts = new HashMap<String, Integer>();
		allTrades.forEach((t, l) -> {
			  medianSpreads1 .putIfAbsent(t, new ArrayList<>(Collections.singletonList(-1.0)));
			  medianSpreads10.putIfAbsent(t, new ArrayList<>(Collections.singletonList(-1.0)));
			  medianSpreads25.putIfAbsent(t, new ArrayList<>(Collections.singletonList(-1.0)));
			  medianSpreads50.putIfAbsent(t, new ArrayList<>(Collections.singletonList(-1.0)));
			  
			  lastMedianSpread1.putIfAbsent (t, -1.0);
			  lastMedianSpread10.putIfAbsent(t, -1.0);
			  lastMedianSpread25.putIfAbsent(t, -1.0);
			  lastMedianSpread50.putIfAbsent(t, -1.0);
			  
			  lastClose.putIfAbsent(t, -1.0);
			  
			  tradeAmounts.put(t, 0);
			  
			});
		
		
		
		for (Map.Entry<String,ArrayList<String>> streamedTrades : allTrades.entrySet()) // now going thru the list ticker by ticker
		{		
			ArrayList<Order> parsedData = new ArrayList<Order>();
			TOHLCVMM result = new TOHLCVMM();
			List<String> trades = new ArrayList<>(streamedTrades.getValue());
			String ticker = streamedTrades.getKey();
			tradeAmounts.put(ticker, streamedTrades.getValue().size());
			String fileTicker = ticker.replace("/", "");
						
			if (medianSpreads1.get(ticker).isEmpty() || medianSpreads10.get(ticker).isEmpty() || medianSpreads25.get(ticker).isEmpty() || medianSpreads50.get(ticker).isEmpty())
			{
				result.medianSpread1  = lastMedianSpread1.get(ticker);
				result.medianSpread10 = lastMedianSpread10.get(ticker);
				result.medianSpread25 = lastMedianSpread25.get(ticker);
				result.medianSpread50 = lastMedianSpread50.get(ticker);
			}
			else
			{
				result.medianSpread1  = Utils.medianDouble(medianSpreads1.get(ticker));
				result.medianSpread10 = Utils.medianDouble(medianSpreads10.get(ticker));
				result.medianSpread25 = Utils.medianDouble(medianSpreads25.get(ticker));
				result.medianSpread50 = Utils.medianDouble(medianSpreads50.get(ticker));
			}
			

			
			if (trades.isEmpty())
			{
				result.timestamp = Instant.now().getEpochSecond();
				double tickerLastClose = lastClose.get(ticker);
				result.open   = tickerLastClose;
				result.high   = tickerLastClose;
				result.low    = tickerLastClose;
				result.close  = tickerLastClose;
				result.volume = 0.0;
			}
			else
			{
				
				for (String data : streamedTrades.getValue()) parsedData.add(Order.getOrderFromLevel2(data)); // NOW: need to edit this to take in the book data. send book data to other helper function. get back the 

				
				result.timestamp = Instant.now().getEpochSecond();
				result.open = parsedData.get(0).price;
				result.close = parsedData.get(parsedData.size() - 1).price;
					
				
				double curHigh = Double.NEGATIVE_INFINITY;
				double curLow = Double.POSITIVE_INFINITY;
				double totalVolume = 0;
				
				for (Order order : parsedData)
				{
					if (order.price > curHigh) curHigh = order.price;
					if (order.price < curLow) curLow = order.price;
					totalVolume += order.price * order.qty;
				}
				
				result.high = curHigh;
				result.low = curLow;
				result.volume = round(totalVolume, 2);

			
			}
			
			lastClose.put(ticker, result.close);
			lastMedianSpread1.put(ticker, result.medianSpread1);
			lastMedianSpread10.put(ticker, result.medianSpread10);
			lastMedianSpread25.put(ticker, result.medianSpread25);
			lastMedianSpread50.put(ticker, result.medianSpread50);
						
			String outputFile = "C:\\Trading\\Data\\Crypto\\KrakenSocket\\" + fileTicker + ".csv";
			
			try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true))) {
				bw.write(result.toString());
	            bw.newLine();

	        } catch (IOException e) { e.printStackTrace(); }
			
		}
		
		tradeAmounts.entrySet().stream().sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder())).forEach(e -> System.out.print(e.getKey() + ": " + e.getValue() + "  "));
		System.out.print("\n");
		
	}

	
	@SuppressWarnings("unused")
	private static double round (double value, int decimalPlaces)
	{
        double multiplier = Math.pow(10, decimalPlaces);
        double roundedValue = Math.round(value * multiplier) / multiplier; 
        return roundedValue;
	}
	
}


class TOHLCVMM
{
	long timestamp;
	double open = 0.0, high = 0.0, low = 0.0, close = 0.0, volume = 0.0, medianSpread1 = 0.0, medianSpread10 = 0.0, medianSpread25 = 0.0, medianSpread50 = 0.0;
	
	TOHLCVMM(){}
    
	TOHLCVMM (long timestamp, double open, double high, double low, double close, double volume, double medianSpread1, double medianSpread10, double medianSpread25, double medianSpread50){ //change this
		this.timestamp      = timestamp;
		this.open           = open;
		this.high           = high;
		this.low            = low;
		this.close          = close;
		this.volume         = volume;
		this.medianSpread1  = medianSpread1;
		this.medianSpread10 = medianSpread10;
		this.medianSpread25 = medianSpread25;
		this.medianSpread50 = medianSpread50;
	}
	
	
	@Override
	public String toString(){ return timestamp+","+open+","+high+","+low+","+close+","+volume+","+medianSpread1+","+medianSpread10+","+medianSpread25+","+medianSpread50; }

}

class Order
{
	String symbol;
	double price, qty;
	
	Order(){}
    
	Order(String symbol, double price, double qty) {
	    this.symbol    = symbol;
	    this.price = price;
	    this.qty = qty;
	}
	
	Order(double price, double qty)
	{
		this.price = price;
		this.qty = qty;
	}
	
	static Order getOrderFromLevel2(String trade)
	{
		Order newOrder = new Order("", 0.0, 0.0);
		
		Pattern pricePattern = Pattern.compile("\"price\"\\s*:\\s*([0-9]*\\.?[0-9]+)");
        Pattern qtyPattern   = Pattern.compile("\"qty\"\\s*:\\s*([0-9]*\\.?[0-9]+)");
        Pattern symbolPattern   = Pattern.compile("\"qty\"\\s*:\\s*([0-9]*\\.?[0-9]+)");

        Matcher mPrice = pricePattern.matcher(trade);
        Matcher mQty   = qtyPattern.matcher(trade);
        Matcher mSym   = symbolPattern.matcher(trade);

        if (mPrice.find() && mQty.find() && mSym.find()) {
            double price = Double.parseDouble(mPrice.group(1));
            double qty   = Double.parseDouble(mQty.group(1));
            String sym   = mSym.group(1);
            newOrder.price = price;
            newOrder.qty = qty;
            newOrder.symbol = sym;
        }

		
		return newOrder;
	}
	
	@Override
	public String toString(){ return symbol + ","+ qty + "," + price; }
	
	public static long dateToTimestamp(String date) {
		// expects date in format: 2025-06-25 19:15:29
	    String[] splitData  = date.split(" ");
	    String[] splitDay   = splitData[0].split("-");
	    String[] splitTime  = splitData[1].split(":");

	    int year   = Integer.parseInt(splitDay[0]);
	    int month  = Integer.parseInt(splitDay[1]);
	    int day    = Integer.parseInt(splitDay[2]);
	    int hour   = Integer.parseInt(splitTime[0]);
	    int minute = Integer.parseInt(splitTime[1]);
	    int second = Integer.parseInt(splitTime[2]);

	    return LocalDateTime
	            .of(year, month, day, hour, minute, second)
	            .toEpochSecond(ZoneOffset.UTC);
	}

}


























































