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

void get_file_name(string inPath, vector<string> &file) {//��ȡ��Ŀ¼�µ�csv�ļ��� 
	inPath += "*";
    long handle;
    struct _finddata_t fileinfo;
    //��һ�β���
    handle = _findfirst(inPath.c_str(),&fileinfo);
    if(handle == -1)
        return;
    do {
        //�ҵ����ļ����ļ���
        string name = fileinfo.name;
        if (name.find(".csv") != name.npos)//�ж��Ƿ���csv�ļ� 
            file.push_back(name);
    } while (!_findnext(handle,&fileinfo));

    _findclose(handle);
}

void insert_values(vector<vector<string>> &data, int s, int e, string &target) {//����sql����е�ֵ����ֵ 
	int i = s;
	for (i = s; i < data.size() - 1 && i < e - 1; ++i) {
		target += "('" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "'),";
	}
	target += "('" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "','" + data[i][0] + "')";
	return;
}

void handle_mysql(vector< vector<string> > &data, int num, MYSQL &mysql) {//����ֵ���� 
    
    string strsql;

    mysql_query(&mysql,"ALTER TABLE table_name DISABLE KEYS");//�ر����� 
    mysql_query(&mysql,"START TRANSACTION");//�������� 
    
    for (int i = 0; i < data.size(); i += num) {
    	
    	strsql = "insert into test_schema values ";
		string values = ""; 
		insert_values(data,  i, i + num, values);
		strsql += values;
		mysql_query(&mysql,strsql.c_str());
		
	}
	
	mysql_query(&mysql,"COMMIT");//�ر����� 
    mysql_query(&mysql,"ALTER TABLE table_name ENABLE KEYS");//��������

    
    return;
}

void get_csv_data(string filepath, string file, vector<vector<string>> &csv_data) {//��ȡ������csv�ļ����� 
	string filename = filepath + file;
    ifstream fp(filename);
    
    string line;
    while (getline(fp,line)){ //ѭ����ȡÿ������
        vector<string> data_line;
        string number;
        istringstream readstr(line); //string��������
        //��һ�����ݰ�'��'�ָ�
        for(int j = 0;j < 11;j++){ //�ɸ������ݵ�ʵ�����ȡѭ����ȡ
            getline(readstr,number,','); //ѭ����ȡ����
            data_line.push_back(number); //�ַ�����int
        }
        csv_data.push_back(data_line); //���뵽vector��
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
    
    if (ConnStatus == NULL) {//�������״̬ 
        // ����ʧ��
        int i = mysql_errno(&mysql);
        string strError= mysql_error(&mysql);
        cout <<"Error info: "<<strError<<endl;
        return 0;
    }
    
    cout<<"Mysql Connected..."<<endl;
    
    
    MYSQL_RES *result=NULL;
    
    int all_rows = readcsv_and_insert(inPath, mysql);
    
    cout<<"insert end"<<endl;
    
    //�ͷŽ���� �ر����ݿ�
    mysql_free_result(result);
    mysql_close(&mysql);
    mysql_library_end();
    
    
    endTime = clock(); 
    cout << "ALL Totle Time : " << (double)(endTime - startTime) / CLOCKS_PER_SEC << "s" << endl;
    return 0;
}

