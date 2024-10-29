import fnmatch

patterns = ['hello*', 'foo', 'regexisfun']
topic = 'regexisfun'

def test():

    for pattern in patterns:
        if fnmatch.fnmatch(topic, pattern):
            return True

    return False

if test():
    print(f"Topic '{topic}' WILL be included")
else:
    print(f"Topic '{topic}' will NOT be included")
