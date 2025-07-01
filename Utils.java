package WC_SCOKET_DATA;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.zip.CRC32;


public class Utils
{
	public static String extractField(String line, int target) {
	    String KEY;
	    switch (target) {
	      case 1: KEY = "\"symbol\":\"";  break;
	      case 2: KEY = "\"price\":";     break;
	      case 3: KEY = "\"qty\":";       break;
	      case 4: KEY = "\"bids\":";      break; // old format bids
	      case 5: KEY = "\"asks\":";      break; // old format asks
	      case 6: KEY = "\"checksum\":";  break;
	      case 7: KEY = "\"bids\":";      break; // new format bids
	      case 8: KEY = "\"asks\":";      break; // new format asks
	      case 9: KEY = "\"req_id\":";    break; // <— new
	      default: return "";
	    }

	    int st = line.indexOf(KEY);
	    if (st < 0) return "";
	    st += KEY.length();

	    if (target == 1) {
	        int en = line.indexOf('"', st);
	        return (en>st) ? line.substring(st, en) : "";
	    }
	    else if (target == 4 || target == 5) {
	        // old single-level array
	        int b0 = line.indexOf('[', st);
	        int b1 = line.indexOf(']', b0);
	        return (b0>=0 && b1>b0) ? line.substring(b0, b1+1) : "";
	    }
	    else if (target == 7 || target == 8) {
	        // new nested array
	        int b0 = line.indexOf('[', st);
	        if (b0 < 0) return "";
	        int depth = 0, end = -1;
	        for (int i = b0; i < line.length(); i++) {
	            char c = line.charAt(i);
	            if (c == '[') depth++;
	            else if (c == ']') {
	                depth--;
	                if (depth == 0) { end = i; break; }
	            }
	        }
	        return (end > b0) ? line.substring(b0, end+1) : "";
	    }
	    else {
	        // numeric fields: price, qty, checksum, and now req_id
	        int comma = line.indexOf(',', st);
	        int brace = line.indexOf('}', st);
	        int en = Math.min(
	            comma < 0 ? Integer.MAX_VALUE : comma,
	            brace < 0 ? Integer.MAX_VALUE : brace
	        );
	        return (en>st) ? line.substring(st, en).trim() : "";
	    }
	}


	
	
	public static List<String> getTickers() {
	    List<String> tickers = new ArrayList<>();
	    Path filePath = Paths.get("C:\\Trading\\Data\\Crypto\\KrakenSocket\\0dataInstruct.csv");
	    try (BufferedReader br = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
	        String line;
	        while ((line = br.readLine()) != null) {
	            line = line.trim();
	            if (line.isEmpty()) continue;

	            // insert slash before the last 3 chars
	            if (line.length() > 3) {
	                int idx = line.length() - 3;
	                String formatted = line.substring(0, idx)
	                                  + "/"
	                                  + line.substring(idx);
	                tickers.add(formatted);
	            } else {
	                // too short to split, just add as-is (or skip)
	                tickers.add(line);
	            }
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    return tickers;
	}
	public static String getSubMessage(String type) {
	    List<String> tickers = new ArrayList<>();
	    Path filePath = Paths.get("C:\\Trading\\Data\\Crypto\\KrakenSocket\\0dataInstruct.csv");
	    
	    // 1) Read & format tickers
	    try (BufferedReader br = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
	        String line;
	        while ((line = br.readLine()) != null) {
	            line = line.trim();
	            if (line.isEmpty()) continue;
	            if (line.length() <= 3) {
	                tickers.add(line);
	            } else {
	                int idx = line.length() - 3;
	                tickers.add(line.substring(0, idx) + "/" + line.substring(idx));
	            }
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    
	    // 2) Build quoted, comma-joined list
	    String symbolList = tickers.stream()
	        .map(t -> "\"" + t + "\"")
	        .collect(Collectors.joining(","));
	    
	    // 3) Choose template
	    String template;
	    if ("trades".equals(type)) {
	        template = """
	            {
	              "method": "subscribe",
	              "params": {
	                "channel":  "trade",
	                "symbol":   [%s],
	                "snapshot": false
	              }
	            }
	            """;
	    } else if ("book".equals(type)) {
	        template = """
	            {
	              "method": "subscribe",
	              "params": {
	                "channel":  "book",
	                "symbol":   [%s],
	                "depth":    10,
	                "snapshot": true
	              }
	            }
	            """;
	    } else {
	        return "";
	    }
	    
	    // 4) Inject and return
	    return String.format(template, symbolList);
	}
	
	public static String getCustomSubMsg(String ticker, String type) {
	    String template = null;
	    if ("trades".equals(type)) {
	        template = """
	            {
	              "method": "subscribe",
	              "params": {
	                "channel":  "trade",
	                "symbol":   ["%s"],
	                "snapshot": false
	              }
	            }
	            """;
	    } else if ("book".equals(type)) {
	        template = """
	            {
	              "method": "subscribe",
	              "params": {
	                "channel":  "book",
	                "symbol":   ["%s"],
	                "depth":    10,
	                "snapshot": true
	              }
	            }
	            """;
	    } else if ("unsub".equals(type)){
	        template = """
	        	      {
	                "method":"unsubscribe",
	                "params":{"channel":"book","symbol":["%s"]}
	              }
	              """;
	    }
	    
	    // 4) Inject and return
	    return String.format(template, ticker);
	}


 class checksumUtils {	 
	 public static long computeChecksum(
			    HashMap<String, String> asks,
			    HashMap<String, String> bids
			) {
			    TreeMap<String, String> sortedAsks = new TreeMap<>(asks);

			    TreeMap<String, String> sortedBids = new TreeMap<>(Collections.reverseOrder());
			    sortedBids.putAll(bids);

			    // 3. build one concatenated string
			    StringBuilder asksStr = new StringBuilder();
			    StringBuilder bidsStr = new StringBuilder();
			    
			    for (var e : sortedAsks.entrySet()) {
			        asksStr.append(formatString(e.getKey()))
			          .append(formatString(e.getValue()));
			    }
			    			    
			    for (var e : sortedBids.entrySet()) {
			    	bidsStr.append(formatString(e.getKey()))
			          .append(formatString(e.getValue()));
			    }
			    
			    StringBuilder sb = asksStr.append(bidsStr);
			    
			    // 4. CRC32 over UTF-8 bytes
			    byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
			  
			    return getCRC32Checksum(bytes);
			}
//	 
	 public static long getCRC32Checksum(byte[] bytes) {
		    // creating an object of the Checksum class.
		    CRC32 crc32 = new CRC32();
		    /*
		    updating the Checksum instance with the bytes from the input.
		    This method replaces the bytes held by the CRC32 Object – this helps with code reuse and negates the need to create new instances of Checksum. 
		    */
		    crc32.update(bytes, 0, bytes.length);
		    // export the checksum.
		    return crc32.getValue();
		}
	 
	 public static String formatString(String plain) {
		    int dot = plain.indexOf('.');
		    String noDot = (dot >= 0)
		        ? plain.substring(0, dot) + plain.substring(dot + 1)
		        : plain;

		    int i = 0, n = noDot.length();
		    while (i < n && noDot.charAt(i) == '0') {
		        i++;
		    }
		    return (i < n) ? noDot.substring(i) : "0";
		}
}	

 class BookUtils {
	 	private static final HttpClient httpClient = HttpClient.newHttpClient();
	 
	    public static void mergeAndTruncate(
	        HashMap<String,String> masterBids,
	        HashMap<String,String> updateBids,
	        HashMap<String,String> masterAsks,
	        HashMap<String,String> updateAsks,
	        int depth
	    ) {
	        applySide(masterBids, updateBids, depth, true);
	        applySide(masterAsks, updateAsks, depth, false);
	    }

	    private static void applySide(
	        Map<String,String> master,
	        Map<String,String> updates,
	        int depth,
	        boolean isBid
	    ) {
	        // 1) merge updates (remove if qty==0, else insert/update)
	        for (Map.Entry<String,String> e : updates.entrySet()) {
	            String priceStr = e.getKey();
	            String qtyStr   = e.getValue();
	            double qty      = Double.parseDouble(qtyStr);
	            if (qty == 0.0) {
	                master.remove(priceStr);
	            } else {
	                master.put(priceStr, qtyStr);
	            }
	        }

	        // 2) truncate to depth if over
	        if (master.size() <= depth) return;
	        List<String> prices = new ArrayList<>(master.keySet());
	        if (isBid) {
	            // bids: worst are lowest prices → sort ascending
	            prices.sort(Comparator.comparingDouble(Double::parseDouble));
	        } else {
	            // asks: worst are highest prices → sort descending
	            prices.sort(Comparator.comparingDouble(Double::parseDouble).reversed());
	        }

	        Iterator<String> it = prices.iterator();
	        while (master.size() > depth && it.hasNext()) {
	            master.remove(it.next());
	        }
	    }
	    

	    /**
	     * Fetches an order‐book snapshot for the given symbol.
	     *
	     * @param symbol e.g. "BTC/USD"
	     * @param depth  max number of price levels to return
	     * @return the raw JSON response from Kraken
	     */
	    public static String fetchSnapshot(String symbol, int depth) {
	        try {
	            String pair = symbol.replace("/", "");
	            String url = "https://api.kraken.com/0/public/Depth"
	                + "?pair=" + URLEncoder.encode(pair, StandardCharsets.UTF_8)
	                + "&count=" + depth;

	            HttpRequest req = HttpRequest.newBuilder()
	                .uri(URI.create(url))
	                .GET()
	                .build();

	            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
	            return resp.body();
	        } catch (Exception e) {
	            throw new RuntimeException("Failed to fetch Kraken snapshot for " + symbol, e);
	        }
	    }

	    public static String resendOrderBook(String symbol) {
	        return fetchSnapshot(symbol, 10);
	    }
	}
}


 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 


















