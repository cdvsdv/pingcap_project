#include <iostream>
#include <mysql.h>
#include <winsock2.h>
#include <windows.h>
#include <bits/stdc++.h>
#include <vector>
#include <fstream>
#include <string.h>
#include <io.h>
#include <thread>

using namespace std;

void get_file_name(string inPath, vector<string> &file) {//获取该目录下的csv文件名 
	inPath += "*";
    long handle;
    struct _finddata_t fileinfo;
    //第一次查找
    handle = _findfirst(inPath.c_str(),&fileinfo);
    if(handle == -1)
        return;
    do {
        //找到的文件的文件名
        string name = fileinfo.name;
        if (name.find(".csv") != name.npos)//判断是否是csv文件 
            file.push_back(name);
    } while (!_findnext(handle,&fileinfo));

    _findclose(handle);
}

void insert_values(vector<vector<string>> &data, int s, int e, string &target) {//批量sql语句中的值插入值 
	int i = s;
	for (i = s; i < data.size() - 1 && i < e - 1; ++i) {
		target += "('" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "'),";
	}
	target += "('" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "')";
	return;
}

void handle_mysql(vector< vector<string> > &data, int num, MYSQL &mysql) {//数据值插入 
    
    string strsql;

    mysql_query(&mysql,"ALTER TABLE table_name DISABLE KEYS");//关闭索引 
    mysql_query(&mysql,"START TRANSACTION");//开启事务 
    
    for (int i = 0; i < data.size(); i += num) {
    	
    	strsql = "insert into test_schema values ";
		string values = ""; 
		insert_values(data,  i, i + num, values);
		strsql += values;
		mysql_query(&mysql,strsql.c_str());
		
	}
	
	mysql_query(&mysql,"COMMIT");//关闭事务 
    mysql_query(&mysql,"ALTER TABLE table_name ENABLE KEYS");//开启索引

    
    return;
}

void get_csv_data(string filepath, string file, vector<vector<string>> &csv_data) {//获取并解析csv文件内容 
	string filename = filepath + file;
    ifstream fp(filename);
    
    string line;
    while (getline(fp,line)){ //循环读取每行数据
        vector<string> data_line;
        string number;
        istringstream readstr(line); //string数据流化
        //将一行数据按'，'分割
        for(int j = 0;j < 11;j++){ //可根据数据的实际情况取循环获取
            getline(readstr,number,','); //循环读取数据
            data_line.push_back(number); //字符串传int
        }
        csv_data.push_back(data_line); //插入到vector中
    }
}

void handle_csv(string filepath, string file, vector<vector<string>> &csv_data, int &all_rows) {
    
    get_csv_data(filepath, file, csv_data);
    all_rows += csv_data.size();
    return;
}

int readcsv_and_insert(string filepath, MYSQL &mysql) {
	int all_rows = 0;
	clock_t startTime,endTime;
    
	vector<string> file;
    get_file_name(filepath, file);
    if (file.empty()) {
        cout << "empty file\n";
        return 0;
    }
    
	vector<vector<string>> csv_data;
	
	for (auto i : file) {
		
		startTime = clock();
    	handle_csv(filepath, i, csv_data, all_rows);
    	endTime = clock();
    	cout << i << " Readed Time : " << (double)(endTime - startTime) / CLOCKS_PER_SEC << "s" << endl;
    	
    	startTime = clock();
    	handle_mysql(csv_data, 40000, mysql);
    	endTime = clock();
    	cout << i << " Insert Time : " << (double)(endTime - startTime) / CLOCKS_PER_SEC << "s" << endl;
    	csv_data.clear();
    	
	}
	
	cout << "ALL Total Row:" << all_rows << endl;
	
	return all_rows;
}


int main() {
    string inPath = "C:\\Users\\Administrator\\Desktop\\pingcap__project\\data\\";
    
    clock_t startTime, endTime;
    startTime = clock();
    
    MYSQL mysql;
    mysql_init(&mysql);
    MYSQL *ConnStatus = mysql_real_connect(&mysql,"localhost","root","","pingcap_project",3306,0,0);
    
    if (ConnStatus == NULL) {//检查连接状态 
        // 连接失败
        int i = mysql_errno(&mysql);
        string strError= mysql_error(&mysql);
        cout <<"Error info: "<<strError<<endl;
        return 0;
    }
    
    cout<<"Mysql Connected..."<<endl;
    
    
    MYSQL_RES *result=NULL;
    
    int all_rows = readcsv_and_insert(inPath, mysql);
    
    cout<<"insert end"<<endl;
    
    //释放结果集 关闭数据库
    mysql_free_result(result);
    mysql_close(&mysql);
    mysql_library_end();
    
    
    endTime = clock(); 
    cout << "ALL Totle Time : " << (double)(endTime - startTime) / CLOCKS_PER_SEC << "s" << endl;
    return 0;
}

