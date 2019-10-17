import csv
import argparse
import urllib.request


class Queue:
    def __init__(self):
        self.items = []
    def is_empty(self):
        return self.items == []
    def enqueue(self, item):
        self.items.insert(0,item)
    def dequeue(self):
        return self.items.pop()
    def size(self):
        return len(self.items)


class Server:
    def __init__(self):
        self.current_request = None
        self.time_remaining = 0

    def tick(self):
        if self.current_request != None:
            self.time_remaining = self.time_remaining - 1
            if self.time_remaining <= 0:
                self.current_request = None

    def busy(self):
        if self.current_request != None:
            return True
        else:
            return False

    def start_next(self,new_request):
        self.current_request = new_request
        self.time_remaining = new_request.get_length()



class Request:
    def __init__(self, time, length):
        self.timestamp = time
        self.length = length

    def get_stamp(self):
        return self.timestamp

    def get_length(self):
        return self.length
    
    def wait_time(self, current_time):
        return current_time - self.timestamp





def simulateOneServer(url):
    
    server = Server()
    queue = Queue()
    waiting_times = []
    data = []

    filename='week5_csvfile.csv'
    
    urllib.request.urlretrieve(url,filename)
    with open(filename, 'rt') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            data.append(row)
    for row in data:
        # if new_print_task():
        simulateTime = int(row[0])
        sim_Length = int(row[2])

        request = Request(simulateTime, sim_Length)
        queue.enqueue(request)
        
        if (not server.busy()) and (not queue.is_empty()):
            next_request = queue.dequeue()
            waiting_times.append(next_request.wait_time(simulateTime))
            server.start_next(next_request)
        
        server.tick()

    average_wait = sum(waiting_times) / len(waiting_times)
    print("Average Wait %6.2f secs %3d tasks remaining."%(average_wait, queue.size()))



def simulateManyServer(url, num_server):
    server = []
    queue = []
    waiting_times = []


    for num in range(0, num_server):
        server.append(Server())
        queue.append(Queue())
        waiting_times.append([])

    data = []

    filename='week5_csvfile.csv'
    
    urllib.request.urlretrieve(url,filename)
    with open(filename, 'rt') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            data.append(row)

    # round-robin fasion for load-balancer
    round_robin = -1

    for row in data:
        # if new_print_task():
        simulateTime = int(row[0])
        sim_Length = int(row[2])

        request = Request(simulateTime, sim_Length)
        queue[round_robin].enqueue(request)

        if round_robin < num_server -1:
            round_robin += 1
        else:
            round_robin = 0    
        
        if (not server[round_robin].busy()) and (not queue[round_robin].is_empty()):
            next_request = queue[round_robin].dequeue()
            waiting_times[round_robin].append(next_request.wait_time(simulateTime))
            server[round_robin].start_next(next_request)
        
        server[round_robin].tick()
    for num in range(0, num_server):
        average_wait = sum(waiting_times[num]) / len(waiting_times[num])
        print("Server :",(num+1))
        print("Average Wait %6.2f secs %3d tasks remaining."%(average_wait, queue[num].size()))

    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help='please enter url')
    parser.add_argument("--servers", help='please enter server no..')
    args = parser.parse_args()

    if args.file:
        if int(args.servers) < 2:
            simulateOneServer(args.file)

        else:
            simulateManyServer(args.file, int(args.servers))        
            pass    
    else:
        print("Exit")
        parser.exit()



if __name__ == "__main__":
    main()





# http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv