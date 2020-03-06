from queue import Queue
from random import randint

class FTQueue:
    def __init__(self):
        self.queue_number = 0
        self.queue_dict = {}
    def qCreate(self, label, size):
        if label not in self.queue_dict:
            self.queue_dict[label] = {}
            self.queue_dict[label]['id'] = self.queue_number
            self.queue_dict[label]['queue'] = Queue(maxsize=size)
            self.queue_number += 1
            return self.queue_dict[label]['id']
        else:
            return self.queue_dict[label]['id']
    
    def qDestroy(self, id):
        for key, value in self.queue_dict.items():
            if value['id'] == id:
                del self.queue_dict['key']
    
        return id
    
    def qId(self, label):
        if label in self.queue_dict:
            return self.queue_dict[label]['id']
    
    def qPush(self, id, item):
        for key, value in self.queue_dict.items():
            print('Id: ', id)
            print('Value[id]: ', value['id'])
            if value['id'] == id:
                queue = value['queue']
                queue.put(item)
                self.queue_dict[key]['queue'] = queue 
    
    def qPop(self, id):

        for key, value in self.queue_dict.items():
            if value['id'] == id:
                queue = value['queue']
                number = queue.get()
                return number

    def qTop(self, id):

        for key, value in self.queue_dict.items():
            if value['id'] == id:
                queue = value['queue']
                number = queue[0]
                return number


    def qSize(self, id):

        for key, value in self.queue_dict.items():
            if value['id'] == id:
                queue = value['queue']
                return queue.qsize()

if __name__ == "__main__":

    q = FTQueue()
    id = q.qCreate('sid', 100)
    q.qPush(id, 35)
    q.qPush(id, 36)
    print('Queue: ', q.qPop(id))
    print('Queue: ', q.qPop(id))
    # print('Queue: ', queue.get())
    # print('Queue: ', list(q.queue_dict['sid']['queue']))
    

