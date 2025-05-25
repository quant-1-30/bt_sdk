def get_data(q):
    data = []
    while True:
        msg = q.get()
        if msg == "eof":
            q.reset()  # 直接重置队列，_is_consumed 会变为 False
            break
        data.append(msg)
    print("get_data completed, queue reset")
    return data 