import requests
proxy_host = "proxy.crawlera.com"
   
proxy_port = "8010"
proxy_auth = "9e4436e8807c4d89b698b1e2272ada85"  # Add your Zyte API key here

proxies = {
   "http": f"http://{proxy_auth}@{proxy_host}:{proxy_port}",
   "https": f"http://{proxy_auth}@{proxy_host}:{proxy_port}",
   }

try:
    response = requests.get("http://httpbin.org/ip", proxies=proxies)
    response.raise_for_status()  # Raise an error for bad responses
    print("Response Status Code:", response.status_code)
    print("Response Content:", response.text)  # Print the raw response content
    print("Response JSON:", response.json())  # Attempt to parse JSON
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")