#更新回数を記録するpyファイル

#from watchdog.observers.polling import PollingObserver
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import os
import pandas as pd
import time

#ファイル名と日時を保存する辞書
#アクセス日時が異なったら辞書の値を変更して，
#フォルダ部分のパスまでを取り出し，アクセス回数を記録する
test_dict = {}

#フォルダ名と更新回数を保存する辞書
folder_dict = {}

#更新されたときにアクセス回数が増えないようにするflag
check_update_flag = False

#wachdogを使用して更新を記録
class DirectoryHandler(FileSystemEventHandler):
    def __init__(self, directory_path):
        super().__init__()
        try:
            self.directory_path = directory_path
            self.file_name = ""
            self.file_changes = 0
        except FileNotFoundError:
            return

    #ファイルが変更されたら
    def on_modified(self, event):
        global check_update_flag

        if event.is_directory:
            return
        self.file_name = os.path.basename(event.src_path)

        #アクセス回数が増加しないようにflagをTrueにする
        #check_update_flag = True
        print(f"file_name: {self.file_name}")
        print(f"{self.directory_path} - {event} - Changes: {self.file_changes}")
        print(check_update_flag)

        if ("tmp" not in self.file_name) and ("~$" not in self.file_name):
            check_file_name = self.file_name #前のイベントのファイル名かを確認する変数

            #バイナリで変更された記録があれば
            if is_binary(event.src_path):
                #初めての更新イベント
                #イベントファイルが変わったら回数を増やすようにする
                if (check_update_flag == False) and (check_file_name != self.file_name):
                    self.file_changes += 1
                    directory_name = os.path.basename(self.directory_path)
                    folder_dict[directory_name][0] += 1
                    check_update_flag = True

                #二回目のファイル更新イベントをスルー    
                elif check_update_flag and (check_file_name == self.file_name):
                    check_update_flag = False
                
                print(check_file_name)
            else:
                self.file_changes += 1
                directory_name = os.path.basename(self.directory_path)
                folder_dict[directory_name][0] += 1
                check_update_flag = True


#引数のファイルがバイナリ形式かどうかを判定する
#b\oはバイナリデータによく含まれるNULLバイトだが，厳密な判定ではない
def is_binary(file_path):
    try:
        with open(file_path, 'rb') as f:
            #バイナリデータを含むかどうかを確認
            return b'\0' in f.read(1024)  #例として最初の1024バイトを読み込む
    except Exception as e:
        print(f"エラー: {e}")
        return False


#指定したファイルの最終アクセス日時を取得
def log_access_time(file_path):
    access_time = os.path.getatime(file_path)
    formatted_time = time.ctime(access_time)
    
    return formatted_time


#引数で指定したディレクトリのファイル情報を取得し，親ディレクトリ情報をfolder_dict，ファイル情報をtest_dictへ保存する
def list_all_files(directory_path, csv_file, directories):
    global test_dict

    #try:
    # 指定したディレクトリの内容を取得
    contents = os.listdir(directory_path)
    print("-" * 20)
    print(f"{directory_path}の中身：{contents}")

    # ファイルとディレクトリをそれぞれ表示
    print("ファイル・ディレクトリ一覧:")
    for content in contents:
        content_path = os.path.join(directory_path, content)

        # ディレクトリの場合はその中のファイルを再帰的に表示
        if os.path.isdir(content_path):
            list_all_files(content_path, csv_file,directories)

        else:
            #既にファイルが存在しているか確認するためのファイルのパス
            file_check = content_path  
            access_time = log_access_time(file_check)
            test_dict[file_check] = access_time
            for directory in directories:
                if directory in content_path:
                    #要素の0番目は更新回数，1番目はアクセス回数
                    folder_dict[directory] = [0, 0]
                
            print("*" * 20)
            print(content_path)  #ファイル・ディレクトリのパスを表示
 
    return test_dict


#アクセス回数を取得
def check_file(file_path, directories):
    global check_update_flag
    access_time = log_access_time(file_path)
    print(f"ファイル名:{file_path}　アクセス日時:{access_time}")
    for directory in directories:
        if directory in file_path:
            folder_name = directory
    
    #アクセス日時が異なっていたらアクセス日時とアクセス回数を更新する
    if test_dict[file_path] != access_time:
        print(f"test_dict[file_path]: {test_dict[file_path]}, access_time: {access_time}")
        test_dict[file_path] = access_time
        if check_update_flag:
            check_update_flag = False
        
        #jpgがある場合desktop.iniがあると，フォルダにアクセスした時点でアクセス回数が増えてしまうため排除
        elif file_path not in "desktop.ini":
            folder_dict[folder_name][1] += 1


#dfの値をcsvファイルに書き込む
def csv_file_write(csv_file, df):
    print(folder_dict)
    for key, values in folder_dict.items():
        new_data = {"フォルダ名":key, "アクセス回数":values[1],"更新回数":values[0]}
        df = pd.concat([df, pd.DataFrame([new_data])], ignore_index=True)
    
    average_access = round(df["アクセス回数"].mean(), 1)
    average_update = round(df["更新回数"].mean(), 1)
    df['アクセス回数の平均'] = [average_access if i == 0 else None for i in range(len(df))]
    df['更新回数の平均'] = [average_update if i == 0 else None for i in range(len(df))]
    print(f"dfの値：{df}")
    df.to_csv(csv_file, index=False,mode="w") #csvファイルに書き込み


if __name__ == "__main__":
    #監視するディレクトリを指定
    base_path = "./"
    directories = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
    print(directories)

    #監視結果を書き込むcsvファイル
    csv_file = "./importance_csv/day1.csv"

    #最初の1行を書き込む
    data = {"フォルダ名":[], "アクセス回数":[],"更新回数":[]}
    df = pd.DataFrame(data)
    print(f"dfの値：{df}")
    df.to_csv(csv_file, index=False,mode="w") #csvファイルに書き込み

    observers = []
    for directory in directories:
        #watchdogによる更新回数を取得する箇所
        #print(f"directoryの中身:{directory}")
        #パスの取得
        path_to_watch = os.path.join(base_path, directory)
        #print(f"path_to_watch：{path_to_watch}")
        event_handler = DirectoryHandler(path_to_watch)
        observer = Observer()
        observer.schedule(event_handler, path=path_to_watch, recursive=True)
        observer.start()
        observers.append(observer)
        #print(f"obserbersの中身：{observer}")

        #ディレクトリ内のファイルのパスとアクセス日時の比較
        list_all_files(path_to_watch, csv_file, directories)

    
    try:
        while True:
            #print(folder_dict)
            #csvファイルの日時を随時読み込む
            for key, value in test_dict.items():
                file_path = key
                check_file(file_path, directories)
                print(f"folder:{folder_dict}")
            time.sleep(3)
            
    except KeyboardInterrupt:
        for observer in observers:
            observer.stop()
        csv_file_write(csv_file, df)
            
    for observer in observers:
        observer.join()