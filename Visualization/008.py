import random,time
def life(current):
    args = random.sample(current,1)[0]
    try:
        if args == 'happiness':
            status = ['happiness']
        else:
            life(current)
        return status
    except:
        time.sleep(random.randint(24,100)*365*24*60*60)

if __name__ == '__main__':
    status = ["Anxiety", "uneasiness", "happiness", "loss", "helplessness"] * 365
    status = life(status)
    print("---"*10)
    print(status)
