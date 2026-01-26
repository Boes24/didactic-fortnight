def countdown_to_seconds(countdown: str) -> int:
    countdown = countdown.lower()
    if countdown == "1d":
        output = 60 * 60 * 24
    elif countdown == "12h":
        output = 60 * 60 * 12
    elif countdown == "8h":
        output = 60 * 60 * 8
    elif countdown == "6h":
        output = 60 * 60 * 6
    elif countdown == "4h":
        output = 60 * 60 * 4
    elif countdown == "2h":
        output = 60 * 60 * 2
    elif countdown == "1h":
        output = 60 * 60
    elif countdown == "30m":
        output = 60 * 30
    elif countdown == "15m":
        output = 60 * 15
    elif countdown == "5m":
        output = 60 * 5
    elif countdown == "3m":
        output = 60 * 3
    elif countdown == "1m":
        output = 60
    else:
        print("Wrong input - timer set to 1 day")
        output = 86400

    return output
