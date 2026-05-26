from twilio.rest import Client

account_sid = "AC4788fab08b574fe536f1e98550891d43"
auth_token = "e5101fb4bc155a59518b82844eab5936"

client = Client(account_sid, auth_token)

call = client.calls.create(
    to="+919082741050",
    from_="+15712523272",
    twiml="""
<Response>
    <Say>Hello Niraj. Twilio test successful.</Say>
</Response>
"""
)

print(call.sid)