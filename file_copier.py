import argparse
import json
import os
import shutil
import sys
from math import floor, log
from os import makedirs, scandir, DirEntry
from os.path import getsize, isfile, join, islink
from queue import Queue
from subprocess import call
from sys import platform, stdout
from threading import Thread
from time import sleep, time
from typing import List, Any, Tuple, Generator

try:
    from tkinter import Button, Message, Scrollbar, StringVar, Text, Tk, TclError, Label
    from tkinter.filedialog import askdirectory
    from tkinter.ttk import Progressbar
except ImportError:
    pass

CACHE = 1 << 26


def _copyfileobj_patched(fsrc, fdst, length=CACHE):
    """Patches shutil method to hugely improve copy speed"""
    while True:
        buf = fsrc.read(length)
        if not buf:
            break
        fdst.write(buf)


shutil.copyfileobj = _copyfileobj_patched
if platform.lower().startswith('win') and getattr(sys, 'frozen', False):
    import ctypes


    def hide_console():
        """Hides the console window in GUI mode. Necessary for frozen application, because this application support both, command line processing AND GUI mode and therefor cannot be run via pythonw.exe."""
        whnd = ctypes.windll.kernel32.GetConsoleWindow()
        if whnd != 0:
            ctypes.windll.user32.ShowWindow(whnd, 0)
            ctypes.windll.kernel32.CloseHandle(whnd)


    def show_console():
        """Unhides console window"""
        whnd = ctypes.windll.kernel32.GetConsoleWindow()
        if whnd != 0:
            ctypes.windll.user32.ShowWindow(whnd, 1)

sizes = ('B', 'KB', 'MB', 'GB', 'TB')
kill = False
running = False
total_size = 0
copied_size = 0
started = 0
copied = 0


class Progress:
    def __init__(self, title='', total=0, value=0, length=0, decimal=2, progress_bar=None, progress_label=None):
        self.length = length
        self.decimal = decimal
        self.title = title
        self.text = ''
        self.__total = total
        self.__value = value
        self.progress_bar = progress_bar
        self.progress_label = progress_label
        self.dynamic = length == 0
        if not progress_bar:
            self.start()

    @property
    def value(self):
        return self.__value

    @value.setter
    def value(self, value):
        self.__value = value
        self.display()

    @property
    def total(self):
        return self.__total

    @total.setter
    def total(self, total):
        global total_size, started, copied_size
        self.__total = total
        if self.progress_bar:
            self.progress_bar['maximum'] = total
            sec = time() - started
            self.progress_label.set(f'{self.value:,} / {total:,}     {self.value / total if total else 0:.2%}     Taken: {calc_time(time() - started)}\n{calc_size(copied_size)} / {calc_size(total_size)}     {copied_size / total_size if total_size else 0:.2%}     ETA: {calc_time((((sec / copied_size) * (total_size - copied_size) if copied_size else 0) + ((sec / self.value) * (self.total - self.value) if self.value else 0)) / 2)}')

    def start(self):
        # self.text = '] Files 0 / 0 00' + ('.' if self.decimal else '') + '0' * self.decimal + '%, Size 0 / 0 00' + ('.' if self.decimal else '') + '0' * self.decimal + '%'
        # self.text = ' ' * (self.length - len(self.text)) + self.text
        stdout.write(f'{self.title}: [')
        self.display()

    def display(self):
        global total_size, started, copied_size
        if self.progress_bar:
            if not kill and self.progress_bar['value'] != self.value:
                self.progress_bar['value'] = self.value
                sec = time() - started
                self.progress_label.set(f'{self.value:,} / {self.total:,}     {self.value / self.total if self.total else 0:.2%}     Taken: {calc_time(time() - started)}\n{calc_size(copied_size)} / {calc_size(total_size)}     {copied_size / total_size if total_size else 0:.2%}     ETA: {calc_time((((sec / copied_size) * (total_size - copied_size) if copied_size else 0) + ((sec / self.value) * (self.total - self.value) if self.value else 0)) / 2)}')
            return
        if self.dynamic:
            self.length = shutil.get_terminal_size().columns - 10
        text = (' Files {:d} / {:d} {:02.' + str(self.decimal) + '%}, Size {} / {} {:02.' + str(self.decimal) + '%}').format(self.value, self.total, self.value / self.total if self.total else 0, calc_size(copied_size), calc_size(total_size), copied_size / total_size if total_size else 0)
        x_char = int((self.value / self.total if self.total else 0) * (self.length - len(text) - 15))
        self.text = '#' * x_char + ' ' * (self.length - x_char - len(text)) + ']' + text
        stdout.write(self.text + chr(8) * len(self.text))
        stdout.flush()

    def finish(self):
        if self.progress_bar:
            self.progress_bar['value'] = 0
            return
        if self.dynamic:
            self.length = shutil.get_terminal_size().columns - 7
        text = '#' * self.length + '] 100%'
        stdout.write(text + ' ' * ((len(self.text) + 5) - len(text)) + '\n')
        stdout.flush()


def fill_queue(q, progress, source='.', destination='.', ignore_exts=None, only_exts=None):
    global total_size
    if not ignore_exts:
        ignore_exts = set()
    elif not isinstance(ignore_exts, set):
        ignore_exts = set(ignore_exts)
    if not only_exts:
        only_exts = set()
    elif not isinstance(only_exts, set):
        only_exts = set(only_exts)
    source = source.replace('\\', '/')
    if destination != '.' and destination[-1] != '/':
        destination += '/'
    if isfile(source):
        progress.total += 1
        ensure_dir(destination)
        dirpath = source.split('/')
        q.put(('/'.join(dirpath[:-1]), dirpath[-1], destination, getsize(source)))
        return
    for dirpath, dirnames, filenames in _walk(source):
        if kill:
            return
        dirpath = dirpath.replace('\\', '/')
        filenames = [f for f in filenames if not ('.' in f.name and ignore_exts and f.name[f.name.rindex('.') + 1:] in ignore_exts)]
        if only_exts:
            filenames = [f for f in filenames if '.' in f.name and f.name[f.name.rindex('.') + 1:] in only_exts]
        progress.total += len(filenames)
        directory = destination if dirpath == source else dirpath.replace(source, destination).replace('//', '/') + '/'
        for file in filenames:
            q.put((dirpath, file.name, directory, file.stat().st_size))


def load_data(filename):
    try:
        with open(filename, 'rt') as file:
            out = set(json.load(file))
        return out
    except FileNotFoundError:
        return set()


def ensure_dir(directory):
    try:
        makedirs(directory)
    except OSError as e:
        if e.errno != 17:
            raise


def save_data(filename, files):
    with open(filename, 'wt') as file:
        json.dump(list(files), file)


def copy(src, dest, errors, alotted_time, size):
    global copied_size, total_size
    start = time()
    try:
        shutil.copy2(src, dest)
        copied_size += size
    except Exception as e:
        errors.append(e.__repr__())
        wait = alotted_time - (time() - start)
        if wait > 0:
            sleep(wait)
        total_size -= size


def _walk(top: str) -> Generator[Tuple[str, List[DirEntry], List[DirEntry]], Any, None]:
    dirs = []
    nondirs = []
    # We may not have read permission for top, in which case we can't get a list of the files the directory contains. os.walk always suppressed the exception then, rather than blow up for a minor reason when (say) a thousand readable directories are still left to visit. That logic is copied here.
    try:
        scandir_it = scandir(top)
    except OSError:
        return
    while True:
        try:
            try:
                entry = next(scandir_it)
            except StopIteration:
                break
        except OSError:
            return
        try:
            is_dir = entry.is_dir()
        except OSError:
            # If is_dir() raises an OSError, consider that the entry is not a directory, same behaviour than os.path.isdir().
            is_dir = False
        if is_dir:
            dirs.append(entry)
        else:
            nondirs.append(entry)
    yield top, dirs, nondirs
    # Recurse into sub-directories
    for name in dirs:
        new_path = join(top, name)
        # Issue #23605: os.path.islink() is used instead of caching entry.is_symlink() result during the loop on os.scandir() because the caller can replace the directory entry during the "yield" above.
        if not islink(new_path):
            for entry in _walk(new_path):
                yield entry


def worker(errors, q, progress, text_output=None):
    global total_size
    while True:
        if kill:
            return
        item = q.get()
        if not item:
            break
        try:
            path = (item[0].replace('\\', '/') + '/' + item[1]).replace('//', '/')
            if not isfile(f'{item[2]}{item[1]}') or item[3] != getsize(f'{item[2]}{item[1]}'):
                total_size += item[3]
                _time = time()
                ensure_dir(item[2])
                size_12 = item[3] >> 15 or 1
                alotted_time = size_12 if size_12 < 86400 else 86400
                t = Thread(target=copy, args=(path, f'{item[2]}/', errors, alotted_time, item[3]), daemon=True)
                t.start()
                calced = calc_size(item[3])
                t.join(alotted_time)
                if t.is_alive():
                    raise TimeoutError(f'"{item[1]}" [{calced}] Failed to copy is the allotted amount of time [{calc_time(alotted_time)}].')
                text = f'"{item[1]}" {calced} Done in {calc_time(time() - _time)}'
                if not progress.progress_bar:
                    stdout.write(f'\r{text}' + ' ' * (progress.length - len(text)) + '\n')
                    stdout.flush()
                    progress.start()
                elif text_output and not kill:
                    text_output.insert('end', f'{text}\n')
                    text_output.see('end')
        except Exception as e:
            text = e.__repr__()
            errors.append(text)
            if not progress.progress_bar:
                stdout.write(f'\r{text}' + ' ' * (progress.length - len(text)) + '\n')
                stdout.flush()
            elif text_output and not kill:
                text_output.insert('end', f'{text}\n')
                text_output.see('end')
        progress.value += 1
        progress.display()
        q.task_done()


def calc_time(seconds=0.0, minutes=0.0, hours=0.0):
    _min, sec = divmod(seconds, 60)
    hour, _min = divmod(_min + minutes, 60)
    return f'{floor(hour + hours)}:{floor(_min)}:{sec:.1f}'


def start_copy(source, destination, ignore, only, output, progress, text=None):
    global kill
    errors = []
    q = Queue()
    workers = [Thread(target=worker, args=(errors, q, progress, text), daemon=True) for _ in range(10)]
    [w.start() for w in workers]
    already = load_data(output) if output else set()
    try:
        fill_queue(q, progress, source, destination, ignore.split(',') if ignore else False, only.split(',') if only else False)
    except Exception as e:
        print(e)
    except KeyboardInterrupt:
        print('killed')
        kill = True
    try:
        while not q.empty() and not kill:
            if all(map(lambda w: not w.is_alive(), workers)) or kill:
                break
            sleep(.1)
        [q.put(None) for _ in workers]
        if not kill:
            [w.join() for w in workers]
    except Exception as e:
        print(e)
    except KeyboardInterrupt:
        print('killed')
        kill = True
    progress.finish()
    if output:
        save_data(output, already)
    return errors


def center(toplevel):
    toplevel.update_idletasks()
    size = tuple(int(_) for _ in toplevel.geometry().split('+')[0].split('x'))
    x = toplevel.winfo_screenwidth() / 2 - size[0] / 2
    y = toplevel.winfo_screenheight() / 2 - size[1] / 2
    toplevel.geometry("%dx%d+%d+%d" % (size + (x, y)))


def calc_size(size):
    bytes_amount = floor(log(size, 2)) if size else 0
    size /= 2 << (10 * (bytes_amount // 10))
    return f'{size:.2f} {sizes[bytes_amount // 10]}'


class MainUI:
    def __init__(self, source, destination, ignore='', only='', output=''):
        self.ignore = ignore
        self.output = output
        self.only = only
        root = self.root = Tk()
        root.title('File Copier')
        root.grid(widthInc=15, baseWidth=450, heightInc=37, baseHeight=150)
        center(root)
        self.source = StringVar(root, source)
        self.destination = StringVar(root, destination)
        Message(root, textvariable=self.source, width=200).grid(column=10, row=0, columnspan=20, sticky='w')
        self.source_btn = Button(root, text='Select Source Folder', command=self.get_source)
        self.source_btn.grid(column=0, columnspan=10, row=0, sticky='w')
        Message(root, textvariable=self.destination, width=200).grid(column=10, row=1, columnspan=20, sticky='w')
        self.destination_btn = Button(root, text='Select Destination Folder', command=self.get_destination)
        self.destination_btn.grid(column=0, row=1, sticky='w', columnspan=10)
        self.text = Text(root, height=10, width=60, font=('Helvetica', 8))
        vsb = Scrollbar(root, orient='vertical', command=self.text.yview)
        self.text.configure(yscrollcommand=vsb.set)
        vsb.grid(column=27, row=2, columnspan=5, rowspan=4, sticky='ns')
        self.text.grid(column=0, row=2, columnspan=27, rowspan=4)
        self.final_button = Button(root, text='Copy', command=self.copy, state='normal' if source and destination else 'disabled')
        self.final_button.grid(column=23, columnspan=5, row=6, sticky='e')
        self.progress = Progressbar(root, orient='horizontal', length=23 * 15, mode='determinate')
        self.progress.grid(columnspan=23, row=6, sticky='w')
        self.progress['maximum'] = self.progress['value'] = 0
        self.progress_label_str = StringVar(root, '')
        self.progress_label = Label(root, textvariable=self.progress_label_str)
        self.progress_label.grid(row=7, columnspan=30)
        root.protocol('WM_DELETE_WINDOW', self.on_closing)
        root.mainloop()

    def on_closing(self):
        def thread():
            global kill, running
            kill = True
            while running:
                sleep(.01)
            sleep(3)
            self.root.quit()

        Thread(target=thread).start()

    def copy(self):
        def run():
            global total_size, started, copied_size, running
            try:
                self.text.delete('1.0', 'end')
            except TclError:
                pass
            running = True
            total_size, copied_size = 0, 0
            self.source_btn['state'] = self.destination_btn['state'] = self.final_button['state'] = 'disabled'
            progress = Progress(progress_bar=self.progress, progress_label=self.progress_label_str)
            started = time()
            errors = start_copy(self.source.get(), self.destination.get(), self.ignore, self.only, self.output, progress, self.text)
            self.text.insert('end', f'{calc_size(copied_size)} of {copied} files copied in ' + calc_time(time() - started))
            progress.finish()
            if errors:
                with open('file_copy_errors.txt', 'wt', encoding='UTF-8') as file:
                    for e in errors:
                        file.write(f'{e}\r\n')
                if sys.platform == 'linux2':
                    call(['xdg-open', 'file_copy_errors.txt'])
                else:
                    os.startfile('file_copy_errors.txt')
            if not kill:
                self.source_btn['state'] = self.destination_btn['state'] = self.final_button['state'] = 'normal'
            running = False

        Thread(target=run).start()

    def get_source(self):
        self.root.update_idletasks()
        folder = askdirectory(initialdir=self.source.get() or '.')
        self.source.set(folder)
        self.final_button['state'] = 'normal' if folder and self.destination.get() else 'disabled'

    def get_destination(self):
        self.root.update_idletasks()
        folder = askdirectory(initialdir=self.destination.get() or '.')
        self.destination.set(folder)
        self.final_button['state'] = 'normal' if self.source.get() and folder else 'disabled'


def console_main(args):
    global total_size, started
    errors = []
    if not args.source:
        errors.append('source')
    if not args.destination:
        errors.append('destination')
    if errors:
        print('error: the following arguments are required: ' + ', '.join(errors))
        exit(2)
    progress = Progress('Copy', decimal=2, length=args.width)
    started = time()
    errors = start_copy(args.source, args.destination, args.ignore, args.only, args.output, progress)
    map(print, errors)
    print(f'{calc_size(copied_size)} of {progress.value} files copied in ' + calc_time(time() - started))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--do_not_copy_file', default='copied.json', metavar='FILE', dest='output')
    parser.add_argument('-i', '--ignore_exts', metavar='EXT,EXT', dest='ignore', default='')
    parser.add_argument('-o', '--only_exts', metavar='EXT,EXT', dest='only', default='')
    parser.add_argument('source', default='', nargs='?')
    parser.add_argument('destination', default='.', nargs='?')
    parser.add_argument('-c', '--nowindow', default=False, action='store_true')
    parser.add_argument('-w', '--console_width', default=0, metavar='WIDTH[0]', dest='width', type=int, help='Width of progress bar and text in console (use 0 for dynamic sizing)')
    pargs = parser.parse_args()
    if pargs.nowindow or 'Tk' not in globals():
        console_main(pargs)
    else:
        if 'hide_console' in globals():
            globals()['hide_console']()
        MainUI(pargs.source, pargs.destination, pargs.ignore, pargs.only)
        if 'show_console' in globals():
            globals()['show_console']()
