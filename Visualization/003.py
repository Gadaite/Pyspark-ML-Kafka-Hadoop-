import datetime
def currenttime(fn):
    fn()
    print(datetime.datetime.now())
    fn()
@currenttime
def printinfo1():
    print("-------------")

@currenttime
def printinfo2():
    print('*************')

@currenttime
def printinfo3():
    print('+++++++++++++')