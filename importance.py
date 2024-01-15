import socket
import pandas as pd
import os
import time
import subprocess
import zipfile

#2回目のsocket通信で受信したデータを格納するリスト
data_lst = []

#バックアップする総データ容量
backup_data_mb = 0

#socket通信でデータを送信する
def send_data(data):
    host = 'ip_address'
    port = 12345

    #socket通信の開始
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))

        client_socket.sendall(data.encode('utf-8'))
        client_socket.close()  # 接続を閉じる


#ディレクトリサイズを取得
def get_directory_size(path):
    total_size = 0
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total_size += entry.stat().st_size
            elif entry.is_dir():
                total_size += get_directory_size(entry.path)

    #サイズを人が読みやすい形式に変換（MB単位）
    total_size = total_size / (1024 * 1024)
    return total_size

#socket通信でデータを受け取る
def get_data(host, port):
    global data_lst

    #socket通信の終わりを知らせるflag
    end_flag = False

    #socket通信の開始
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server2_socket:
        server2_socket.bind((host, port))
        server2_socket.listen()

        print(f"サーバが {host}:{port} で待機中")
        
        while True:
            conn, addr = server2_socket.accept()
            with conn:
                print('クライアントが接続しました:', addr)

                while True:
                    data = conn.recv(1024)
                    if not data:
                        print("クライアントが接続を閉じました。")
                        break
                    elif data.decode('utf-8') == "END":
                        end_flag = True
                        print("クライアントが終了を通知しました。サーバを停止しません。")
                    else:
                        received_data = data.decode('utf-8')
                        add_data = received_data.split(",")
                        data_lst.append(add_data)
                        print(f"受信したデータ: {received_data}")
                        
                        #送信先のデータがなくなったらwhile文を終了
                        if received_data == "NO_MORE_DATA":
                            break
                if end_flag:
                    break


#バックアップ操作
def start_buckup(data_lst):
    #バックアップサーバの容量の30%
    limit_capacity = float(data_lst[-2][0])
    #float(data_lst[-2][0])
    #バックアップサーバの使用可能容量
    capacity_mb = float(data_lst[-1][0])
    #増分バックアップ分のバックアップデータ量
    add_data_size = float(data_lst[-3][0])

    for data in data_lst:

        print(data)
        print(f"limit_capacity:{limit_capacity}, capacity_mb + add_data_size:{capacity_mb + add_data_size}")
        
        #dataの最後から2つはデータサイズのためパス
        if len(data) > 1:
            #フォルダのパス
            folder_path = data[0]
            #バックアップの1つのデータサイズ
            data_size = data[2]

            #バックアップサーバの容量の70%に収まっていれば
            if capacity_mb + add_data_size < limit_capacity:
                print(f"現在のcapacity_mb:{capacity_mb}MB")
                #非圧縮で送信
                print(os.path.exists(folder_path))
                data_transfer(folder_path)
                
            else:
                #圧縮して送信
                # 圧縮後のZIPファイル
                zip_file_path = folder_path + ".zip"

                # 圧縮実行
                compress_directory(folder_path, zip_file_path)
                
                #実験ではここを無くす
                #zipの方が大きくなるものを無視しているため
                if get_directory_size(folder_path) > os.path.getsize(zip_file_path) / (1024 * 1024):
                    #増分バックアップ分を圧縮したバージョンに変更する
                    add_data_size -= get_directory_size(folder_path)
                    add_data_size += os.path.getsize(zip_file_path) / (1024 * 1024)

                data_transfer(zip_file_path)


#scpでデータを送信
def data_transfer(local_folder_path):
    #バックアップデータを保存するディレクトリを指定
    remote_folder_path = "/home/"
    user = "user_name"
    host = 'ip_address'

    # `scp -r`コマンドを実行
    scp_command = f"scp -r {local_folder_path} {user}@{host}:{remote_folder_path}"

    try:
        subprocess.run(scp_command, shell=True, check=True)
        print("SCP command executed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Error executing SCP command. Error: {e}")


#データを圧縮する
def compress_directory(directory_path, zip_path):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, directory_path)
                zipf.write(file_path, arcname=arc_name)


if __name__ == "__main__":
    #重要度を決定したフォルダのあるディレクトリ
    base_directory = './'
    #重要度を決定するCSVを保存するディレクトリを指定
    csv_folder = './importance_csv'
    csv_file_lst = file_list = [f for f in os.listdir(csv_folder) if os.path.isfile(os.path.join(csv_folder, f))]
    #1週間分のアクセス・更新回数を保存する変数
    total_access = 0
    total_update = 0
    
    #送るデータ総サイズ
    total_data_size = 0
    host = '192.168.100.102'
    port = 56789

    #csvフォルダからアクセス回数，更新回数の平均を計算
    for csv_file_path in csv_file_lst[-6:-2]:
        csv_file_path = os.path.join(csv_folder, csv_file_path)
        df = pd.read_csv(csv_file_path)
        total_access += round(df['アクセス回数'].sum())
        total_update += round(df['更新回数'].sum())
    d_value = total_access / 5
    e_value = total_update / 5

    target_csv_file = os.path.join(csv_folder, csv_file_lst[-1])
    df = pd.read_csv(target_csv_file)

    #csvからデータを読み取ってデータを送信する
    for folder_name, group_df in df.groupby(df.columns[0]):
        result_b_d = group_df['アクセス回数'].mean() / d_value
        result_c_e = group_df['更新回数'].mean() / e_value

        result_sum = (result_b_d + result_c_e) * 100
        result_ans = round(result_sum * 0.25)

        folder_path = os.path.join(base_directory, folder_name)

        folder_size = get_directory_size(folder_path)
        total_data_size += folder_size

        print(f'フォルダ名：{folder_path}, 優先度の値：{result_ans}, フォルダサイズ: {folder_size}MB')

        #data_to_send = f"フォルダ名：{folder_path}, 優先度の値：{result_ans}, トータルフォルダサイズ: {total_data_size}MB"
        data_to_send = f"{folder_path},{result_ans},{total_data_size}"
        
        print(f"send_data:{send_data}")
        send_data(data_to_send)

    # 全てのデータを送信したら、終了をサーバに通知
    print("END")
    send_data("END")
    end_flag = True
    time.sleep(1)

    #socketが終了したら
    if end_flag:
        get_data(host, port)
    print(f"受信したデータのリスト:{data_lst}")

    #バックアップ操作の開始
    start_buckup(data_lst)