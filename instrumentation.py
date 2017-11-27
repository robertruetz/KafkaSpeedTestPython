__author__ = 'RRuetz'
import datetime as dt


class Timer(object):

    def __init__(self, start=False):
        self.startTime = None
        self.endTime = None
        self.delta = None
        self.marks = {}
        if start:
            self.start()

    def start(self):
        self.startTime = dt.datetime.now()
        self.marks['start'] = self.startTime

    def stop(self):
        if self.startTime is not None:
            self.endTime = dt.datetime.now()
            self.delta = self.get_delta(self.startTime, self.endTime)
        else:
            print("Timer has not been started yet.")

    def check(self):
        """
        returns a datetime.timedelta object that represents the elapsed time since the Timer started
        """
        if self.startTime is not None:
            return dt.datetime.now() - self.startTime
        else:
            print("Timer has not been started yet.")

    @staticmethod
    def get_delta(startTime, endTime):
        if endTime is not None and startTime is not None:
            delta = endTime - startTime
            return delta
        else:
            print(r"No delta can been established. Did you forget to start and/or stop your timer?")

    def to_string(self):
        if self.delta is not None:
            return "{0}".format(self.delta.seconds * 1000 + self.delta.microseconds / 1000)
        else:
            print(r"No time delta has been established. Did you forget to start and/or stop your timer?")

    def mark(self, name):
        """
        Allows user to mark smaller increments of time within a timer span.
        """
        t = dt.datetime.now()
        mark = Timer()
        mark.startTime = self.marks.get('last', None)
        if mark.startTime is None:
            mark.startTime = self.startTime
        mark.endTime = t
        mark.delta = mark.endTime - mark.startTime
        self.marks[name] = mark
        self.marks['last'] = mark.endTime

# a global timer that we start at the beginning of operations. can be imported and interacted with across the project.
GLOBAL_TIMER = Timer()
