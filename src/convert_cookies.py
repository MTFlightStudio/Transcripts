import json
import os

# Load the JSON cookies file
with open('youtube_cookies.txt', 'r') as file:
    cookies = json.load(file)

# Create a Netscape formatted cookies file
with open('youtube_cookies_netscape.txt', 'w') as file:
    # Write header
    file.write("# Netscape HTTP Cookie File\n")
    file.write("# https://curl.se/docs/http-cookies.html\n")
    file.write("# This is a generated file. Do not edit.\n\n")
    
    # Write each cookie in Netscape format
    for cookie in cookies:
        # Skip session cookies (they have no expiration)
        if cookie.get('session', False) and 'expirationDate' not in cookie:
            expiry = 0
        else:
            expiry = int(cookie.get('expirationDate', 0))
        
        # Determine if subdomains can use this cookie
        hostOnly = 'FALSE' if cookie.get('hostOnly', False) else 'TRUE'
        
        # Determine if cookie requires secure connection
        secure = 'TRUE' if cookie.get('secure', False) else 'FALSE'
        
        # Format and write the cookie line
        # domain flag path secure expiry name value
        cookie_line = f"{cookie['domain']}\t{hostOnly}\t{cookie['path']}\t{secure}\t{expiry}\t{cookie['name']}\t{cookie['value']}\n"
        file.write(cookie_line)

print("Converted cookies to Netscape format in 'youtube_cookies_netscape.txt'") 