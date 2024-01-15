import socket
import os
import time

#受け取った値を格納するためのリスト
data_lst = []

def start_server(host, port):
    
    global data_lst
    #socket通信を終了するためのflag
    end_flag = False

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()

        print(f"サーバが {host}:{port} で待機中")

        while True:
            conn, addr = server_socket.accept()
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
        #conn.close()
        #server_socket.close()

#サーバの空き容量の確認               
def capacity_check(dir_path):
    dir_path = "./"
    stat = os.statvfs(dir_path)
    capacity_mb = stat.f_bsize * stat.f_bfree / 1024 /  1024
    
    return capacity_mb


#バックアップサーバの総容量を取得して，30%を返す
def get_total_disk_capacity(path='/'):
    statvfs_result = os.statvfs(path)

    # ブロックサイズ * 総ブロック数 / 1024でKB単位に変換
    total_capacity_kb = (statvfs_result.f_frsize * statvfs_result.f_blocks) / 1024

    # KBをMB単位に変換
    total_capacity_mb = total_capacity_kb / 1024

    #30%を計算
    limit_capacity = total_capacity_mb * 0.9

    return limit_capacity


#socket通信でデータを送信する
def send_data(data):
    host = '192.168.100.102'
    port = 56789

    #リスト内の各要素を文字列に変換し、カンマで結合
    if data != "END":
        if type(data) == float:
            data_str = str(data)
        else:
            data_str = ",".join(map(str, data))
    else:
        data_str = data

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        print("part1")

        client_socket.sendall(data_str.encode('utf-8'))
        print("part2")
        client_socket.close()  # 接続を閉じる


#対象フォルダが圧縮されているかどうかの確認
#Trueの場合，非圧縮状態
def Check_compression(folder_path):
    #バックアップサーバ（VM）内のディレクトリを指定
    base_path = "/home/"
    directories = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
    new_folder_path = ""

    if folder_path in directories:
        return True
    elif folder_path in ".zip":
        for i in range(len(folder_path)):
            if folder_path[i] == ".":
                break
            else:
                new_folder_path += folder_path[i]
        if new_folder_path in directories:
            return True
        else:
            return False


if __name__ == "__main__":
    host = 'ip_address'  # クライアント側のIPアドレス
    port = 12345              # クライアント側のポート番号
    dir_path = "./"
    #バックアップサーバの使用可能容量の取得
    capacity_mb = capacity_check(dir_path)
    compression_flag = False

    start_server(host, port)
    
    #送られてきたデータの最終行が最大データサイズのため，その部分を取得
    add_data_size = float(data_lst[-1][2])
    
    sorted_data = sorted(data_lst, key=lambda x: int(x[1])) #reverse = Trueをいれると高い順に並び替える

    #圧縮されているかの確認
    for data in sorted_data:
        folder_name =  os.path.basename(data[0])
        print(f"folder_path:{folder_name}")
        comp_flag = Check_compression(folder_name)
        data.append(comp_flag)
        print(data)
    
    #実験の場合はこの符号逆
    if capacity_mb > add_data_size:
        print("yeah")
    else:
        print("why")
    
    #フォルダパス，重要度，データサイズ（1つ以外関係なし），wbsファイルの状態，バックアップサーバ側の圧縮状態
    print(sorted_data)
    print(len(sorted_data))
    print(add_data_size)
    print(capacity_mb)

    #データの送信
    time.sleep(3)

    #ファイルサーバへ送信
    for data in sorted_data:
        send_data(data)
    print("END")
    limit_capacity = get_total_disk_capacity("/")
    send_data(add_data_size)
    send_data(limit_capacity)
    send_data(capacity_mb)
    send_data("END")