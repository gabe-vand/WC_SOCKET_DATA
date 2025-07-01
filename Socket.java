import java.net.URI;
import java.net.http.HttpClient;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.net.http.WebSocket;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HttpsURLConnection;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.MessageDigest;
//NOTE: Code is based on this -> https://support.kraken.com/hc/en-us/articles/4883459626132-Example-code-for-Java-REST-and-WebSocket-API

class BaseSocket {
	public Integer pingId;
	private static ConcurrentHashMap<String, ArrayList<String>> trades = new ConcurrentHashMap<String, ArrayList<String>>();
	protected static ConcurrentHashMap<String, OrderBook> orderBooks = new ConcurrentHashMap<String, OrderBook>();
	private static final List<Integer> pings = Collections.synchronizedList(new ArrayList<>());
	private static final List<String> badChecksums = Collections.synchronizedList(new ArrayList<>());
	public static boolean connected = true;
    protected String url;
    protected String subscription;
    private static boolean filter_heartbeat;
    protected WebSocket ws;   
    public static String errorSym = "";
    
    public static List<Integer> getPings()
    {
    	return pings;
    	
    }
    
    public static List<Integer> clearPings()
    {
    	pings.clear();
    	return pings;
    }
    
    public static List<String> getBadChecksums()
    {
    	return badChecksums;
    }
    
    public static List<String> clearBadChecksums()
    {
    	badChecksums.clear();
    	return badChecksums;
    }
    
    public ConcurrentHashMap<String, ArrayList<String>> getTrades() {
 
        ConcurrentHashMap<String, ArrayList<String>> copy = new ConcurrentHashMap<>(BaseSocket.trades.size());
        for (Map.Entry<String, ArrayList<String>> entry : BaseSocket.trades.entrySet()) {
            copy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return copy;
    }
    
    public void clearTrades()
    {
    	 BaseSocket.trades.clear();
    }
    
    public ConcurrentHashMap<String, OrderBook> getBooks() {
    	/*
    	 * Deep copies the orderBook.
    	 */
        ConcurrentHashMap<String, OrderBook> copy =
            new ConcurrentHashMap<>(BaseSocket.orderBooks.size());

        for (Map.Entry<String, OrderBook> entry : BaseSocket.orderBooks.entrySet()) {
            String symbol    = entry.getKey();
            OrderBook orig   = entry.getValue();

            OrderBook clone  = new OrderBook(orig);

            copy.put(symbol, clone);
        }

        return copy;
    }
    
    public void clearOrderBook() {
        orderBooks.clear();
    }

    public BaseSocket(String subscription, boolean filter_heartbeat)
    {
    	/*
    	 * Base socket does nothing without running OpenAndStreamWebSocketSubscription (usually in another thread), see runSocket.stream()
    	 */
        this.subscription = subscription;
        BaseSocket.filter_heartbeat = filter_heartbeat;

    }
    
    public void reconnect() {
        System.out.println("Reconnecting socket…");
        try {
            if (ws != null) {
                ws.sendClose(1000, "reconnect").join();
            }
        } catch (Exception e) {
            System.err.println("Error closing socket: " + e.getMessage());
        }
        // now open a fresh one
        OpenAndStreamWebSocketSubscription();
    }

    public void OpenAndStreamWebSocketSubscription() 
    /*
     * Opens a websocket. see 'ontext' callback function for reading inputs, and onerror callback for reading errors, as well as onopen
     */
    {
        try {
            CountDownLatch latch = new CountDownLatch(1);
            ws = HttpClient                    // assign to the field
                .newHttpClient()
                .newWebSocketBuilder()
                .buildAsync(URI.create(this.url), new WebSocketClient())
                .join();

            ws.sendText(this.subscription, true);
            latch.await();
        } catch (Exception e) {
            System.err.println("Error: " + e);
        }
    }
    
    private static class WebSocketClient implements WebSocket.Listener
    {  	
 

		@Override
        public void onOpen(WebSocket webSocket)
        /*
         * Configure activity triggered by a socket opening.
         */
		{
            //System.out.println("Socket connected.");
			WebSocket.Listener.super.onOpen(webSocket);
        }
		
		

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last)
        /*
         * IMPORTANT!! This callback function recieves all of the server data 'data' and is called each time a message is recieved.
         * Currently, this function filters messages based on their type. Imporantly, we have 'book' and 'trade'.
         * Trades and OrderBook updates are automatically added to their hashmaps, with the market identifier ("BTC/USD") as the key.
         * 
         */
        {
        	
        	DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime nowUtc = LocalDateTime.now(Clock.systemUTC());
            String dataStr = dtf.format(nowUtc) + ": " + data + "\n";
            String symbol ="";
            try { symbol = Utils.extractField(dataStr, 1); } catch (Exception e) { }
            
            
            if (!(filter_heartbeat && dataStr.contains("heartbeat")) && !(dataStr.contains("subscribe")) && !(dataStr.contains("status")))
			{            	
               //System.out.println(dtf.format(nowUtc) + ": " + data + "\n");
                
                if (dataStr.contains("req_id"))pings.add(Integer.parseInt(Utils.extractField(dataStr, 9)));

                else if (symbol.equals("")) return WebSocket.Listener.super.onText(webSocket, data, false);
                                
                else if (dataStr.contains("book"))
            	{
                	orderBooks.computeIfAbsent(symbol, k -> new OrderBook(k));
                	
                	if (dataStr.contains("snapshot")) orderBooks.get(symbol).clear();
                		
					boolean goodChecksum = orderBooks.get(symbol).apply(dataStr, symbol);
					
					if (!goodChecksum && (!badChecksums.contains(symbol))) badChecksums.add(symbol); 				

                }
                
                else if (dataStr.contains("trade"))
                {
                	trades.computeIfAbsent(symbol, sym -> new ArrayList<String>());
                	trades.get(symbol).add(dataStr);
                	//System.out.println(dataStr);
                }
                
                
                else
                {
                	//System.out.println(dtf.format(nowUtc) + ": " + data + "\n"); //this is usually goofy weird data that is missing its front half
                }
            }
            return WebSocket.Listener.super.onText(webSocket, data, false);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            System.err.println("ERROR OCCURRED: " + error.getMessage());
            error.printStackTrace();

        }
        
    }

}

class OrderBook {
	/*
	 * Representation of the order book for a singe market.
	 * Does not hold order objects, but rather maps string prices to string quantites. 
	 * This is for ease of comparison, and because the checksum needs string formatting (preservation of trailing zeroes is important).
	 */
    public final String ticker;
    private final HashMap<String, String> bids = new HashMap<>();
    private final HashMap<String, String> asks = new HashMap<>();

    public OrderBook(String ticker)
    {
        this.ticker = ticker;
    }
    
    public void clear()
    {
		bids.clear();
		asks.clear();
		
	}

	public OrderBook(OrderBook other)
	/*
	 * clone constructor - copy an order book
	 */
	{
        this.ticker = other.ticker;
        this.bids.putAll(other.bids);
        this.asks.putAll(other.asks);
    }

	public boolean apply(String json, String symbol)
	/*
	 * Most important method in the order book management.
	 * Takes in a raw json string directly from the websocket, and modifies the bids and asks of the orderbook based on the new data
	 * the first block with extractfield and getorderfromstring are used to turn the json string into Hashmap<String, String> represeting the new bids/ask
	 * Merge and truncate applies the kraken order book management algorithmn to update the book
	 * Returns 'false' if the parsed server checksum and the calcualted local checksum are not equivalent.
	 */
	{
		long server = -1;
		try { server = Long.parseLong(Utils.extractField(json, 6)); }
        catch (Exception e) { System.err.println("ERROR PARSING: " + json); }
		
	    String bidsLine, asksLine;
	    HashMap<String,String> bidsOrders, asksOrders;

        bidsLine  = Utils.extractField(json, 4);  
        asksLine  = Utils.extractField(json, 5);   
        bidsOrders = getOrderFromString(bidsLine);
        asksOrders = getOrderFromString(asksLine);

	    BookUtils.mergeAndTruncate(bids, bidsOrders, asks, asksOrders, 10);

        long local = checksumUtils.computeChecksum(asks, bids);
        
        if (server != local)
        {
            System.err.println(symbol + " checksum mismatch: server=" + server + " local=" + local);
            return false;
        }

	    return true;
	}
    
    public static HashMap<String, String> getOrderFromString(String line)
    /*
     * Crude json parser used to turn a string of bids/ask data (formatted very specifically) into HashMap<String, String> mapping prices to quantities.
     */
    {
        if (line == null || line.equals("[]") || line.length() < 3) {
            return new HashMap<>();
        }
        HashMap<String, String> orders = new HashMap<>();

        String inner = line.substring(1, line.length() - 1);
        String[] parts = inner.split("\\},\\{");

        for (String p : parts) {
            p = p.startsWith("{") ? p : "{" + p;
            p = p.endsWith("}")   ? p : p + "}";
            int pi = p.indexOf("\"price\":") + 8;
            int pc = p.indexOf(",", pi);
            String price = p.substring(pi, pc);
            int qi = p.indexOf("\"qty\":", pc) + 6;
            int qb = p.indexOf("}", qi);
            String qty   = p.substring(qi, qb);
            orders.put(price, qty);
        }

        return orders;
    }

	public HashMap<String, String> getBids() { return bids; }
    public HashMap<String, String> getAsks() { return asks; }
}
	
class PublicSocket extends BaseSocket {
    PublicSocket(String subscription, boolean filter_heartbeat, String URL) {
        super(subscription, filter_heartbeat);
        this.url = URL;
    }
}

class PrivateSocket extends BaseSocket {
    String apiPublicKey = "";
    String apiPrivateKey = "";

    // no more subscription parameter here
    PrivateSocket(String subscription, boolean filter_heartbeat) {
        super(subscription, filter_heartbeat);
        this.url = "wss://ws-auth.kraken.com/v2";
        initialize();
    }

    private void initialize() {
        // 1) fetch the token via REST
        String tokenQuery = QueryPrivateEndpoint(
            "GetWebSocketsToken",
            "",
            apiPublicKey,
            apiPrivateKey
        );

        // 2) crude JSON parse to pull out "token"
        String token = "";
        int i = tokenQuery.indexOf("\"token\":\"");
        if (i != -1) {
            int start = i + 9;
            int end   = tokenQuery.indexOf("\"", start);
            token = tokenQuery.substring(start, end);
        } else {
            throw new RuntimeException("No token in: " + tokenQuery);
        }

        // 3) build the v2 JSON-RPC subscribe message
        this.subscription = subscription.formatted(token);
    }

     // Private REST API Endpoints
    public static String QueryPrivateEndpoint(String endPointName, String inputParameters, String apiPublicKey, String apiPrivateKey) {
        String responseJson = "";

        String baseDomain = "https://api.kraken.com";
        String privatePath = "/0/private/";

        String apiEndpointFullURL = baseDomain + privatePath + endPointName + "?" + inputParameters;

        String nonce = String.valueOf(System.currentTimeMillis());

        String apiPostBodyData = "nonce=" + nonce + "&" + inputParameters;

        String signature = CreateAuthenticationSignature(apiPrivateKey, privatePath, endPointName, nonce, apiPostBodyData);

        // CREATE HTTP CONNECTION
        try {
            HttpsURLConnection httpConnection = null;

            @SuppressWarnings("deprecation")
			URL apiUrl = new URL(apiEndpointFullURL);

            httpConnection = (HttpsURLConnection) apiUrl.openConnection();

            httpConnection.setRequestMethod("POST");

            httpConnection.setRequestProperty("API-Key", apiPublicKey);

            httpConnection.setRequestProperty("API-Sign", signature);

            httpConnection.setDoOutput(true);

            DataOutputStream os = new DataOutputStream(httpConnection.getOutputStream());

            os.writeBytes(apiPostBodyData);

            os.flush();

            os.close();

            BufferedReader br = null;

            // GET JSON RESPONSE DATA
            br = new BufferedReader(new InputStreamReader((httpConnection.getInputStream())));

            String line;

            while ((line = br.readLine()) != null) {
                responseJson += line;
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        return responseJson;
    }

    // Authentication Algorithm
    public static String CreateAuthenticationSignature(String apiPrivateKey, String apiPath, String endPointName, String nonce, String apiPostBodyData) {
        try {
            // GET 256 HASH
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update((nonce + apiPostBodyData).getBytes());
            byte[] sha256Hash = md.digest();

            // GET 512 HASH
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(Base64.getDecoder().decode(apiPrivateKey.getBytes()), "HmacSHA512"));
            mac.update((apiPath + endPointName).getBytes());

            // CREATE API SIGNATURE
            String signature = new String(Base64.getEncoder().encodeToString(mac.doFinal(sha256Hash)));

            return signature;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }
}