class ProgressBar:
    def __init__(self,total_tasks):
        self.total_tasks = total_tasks
        self.done_tasks = 0
        self.todo_tasks = 0
        self.progress_pct = 0
        self.progress_bar = ""
        self.todo_char = "."
        self.done_char = ">"
        self.bar_length = 20
        self.todo_len = ""
        self.done_len = ""
        self.bar = ""
        

    def calc_progress(self):
        self.progress_pct = int(self.done_tasks / self.total_tasks * 100)
        self.todo_tasks = self.total_tasks - self.done_tasks
    
    def calc_bar(self):
        self.done_len = int(self.bar_length / self.total_tasks * self.done_tasks )
        self.todo_len = self.bar_length - self.done_len
        # set the bar with the concate 
        self.bar = (self.done_char * self.done_len) + (self.todo_char * self.todo_len)
    


    def set_progress_bar(self):
        self.progress_bar = '[{}] {}/{} [{}%]'.format(self.bar,self.done_tasks,self.total_tasks,self.progress_pct)

    def get_progress(self):
        self.calc_bar()
        self.calc_progress()
        self.set_progress_bar()

        return self.progress_bar