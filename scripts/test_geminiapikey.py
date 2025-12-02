import google.generativeai as genai, os
genai.configure(api_key="AIzaSyAN5_w0GSfdUL0sca-UUMqAL77fOipnv50")
model = genai.GenerativeModel("gemini-2.5-flash")
resp = model.generate_content("Say hello in one sentence.")
print(resp.text)