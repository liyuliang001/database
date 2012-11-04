#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <cassert>

#include "../include/client.h"
#include "../tool/tokenize.h"
#include "../tool/split_csv.h"

using namespace std;

int s2i(string s){
	int res=0;
	for (int i=0;i<s.size();i++)
		res=res*10+s[i]-'0';
	return res;
}


class condition{
public:
	int type;
	string left,right;
	int constant;
	char op;
	condition(string a,char c,string b){
		left=a;
		op=c;
		if (b[0]>='0'&&b[0]<='9'){
			constant=s2i(b);
			type=0;
		}else if (b[0]=='\''){
			right=b.substr(1,b.size()-2);
			type=1;
		}else {
			right=b;
			type=2;
		}
	}
};

map<string, vector<string> > table2name;
map<string, vector<string> > table2type;
map<string, vector<string> > table2pkey;
vector<string> result;

void done(const vector<string>& table, const map<string, int>& m,
	int depth, vector<string>& row,vector<condition> &cond)
{
	if (depth==table.size()){
		for (int i=0;i<cond.size();i++){
			if (cond[i].type==2){
				string a=row[m.find(cond[i].left)->second],b=row[m.find(cond[i].right)->second];
				if (a!=b)return;
			}
		}
		string s=row[0];
		for (int i=1;i<row.size();i++){
			s+=","+row[i];
		}
		result.push_back(s);
		return;
	}
	FILE* fin;
	char buf[65537];
	vector<string> token,column_name;
	
	fin = fopen(((string) "data/" + table[depth]).c_str(), "r");
	column_name=table2name[table[depth]];
	map<string,int> names;
	vector<condition> cond_now;
	for (int i=0;i<column_name.size();i++)names[column_name[i]]=i;
	for (int i=0;i<cond.size();i++){
		if (cond[i].type<2&&names.find(cond[i].left)!=names.end()){
			cond_now.push_back(cond[i]);
		}
	}
	
	while (fgets(buf,sizeof(buf),fin)!=NULL){
		int len=strlen(buf);
		if (len==0)continue;
		if (buf[len-1]=='\n')buf[--len]=0;
		split_csv(buf,token);
		
		bool ok=true;
		
		for (int i=0;i<cond_now.size();i++){
			if (cond_now[i].type==0){
				int a=s2i(token[names.find(cond_now[i].left)->second]);
				switch (cond_now[i].op){
					case '=':
						if (a!=cond_now[i].constant)ok=false;
						break;
					case '<':
						if (a>=cond_now[i].constant)ok=false;
						break;
					case '>':
						if (a<=cond_now[i].constant)ok=false;
						break;
				}
			}else if (cond_now[i].type==1){
				string a=token[names.find(cond_now[i].left)->second];
				a=a.substr(1,a.size()-2);
				if (a!=cond_now[i].right)ok=false;
			}
			if (!ok)break;
		}
		
		if (!ok)continue;
		
		for (int i=0;i<column_name.size();i++)if (m.find(column_name[i])!=m.end()){
			row[m.find(column_name[i])->second]=token[i];
		}
		done(table,m,depth+1,row,cond);
	}
	fclose(fin);
}

void create(const string& table, const vector<string>& column,
	const vector<string>& type, const vector<string>& key)
{
	table2name[table] = column;
	table2type[table] = type;
	table2pkey[table] = key;
}

void train(const vector<string>& query, const vector<double>& weight)
{
	// I am too clever; I don't need it.
}

void load(const string& table, const vector<string>& row)
{
	FILE *fout;
	int i;

	fout = fopen(((string) "data/" + table).c_str(), "w");
	assert(fout != NULL);

	for (i = 0; i < row.size(); i++)
		fprintf(fout, "%s\n", row[i].c_str());

	fclose(fout);
}

void preprocess()
{
	// I am too clever; I don't need it.
}

void execute(const string& sql)
{
	
	vector<string> token;
	
	tokenize(sql.c_str(), token);
	if (token[0]=="SELECT"){
		vector<string>  output, table, row;
		vector<condition> cond;
		map<string, int> m;
		result.clear();
		output.clear();
		table.clear();
		int i;
		for (i=1;i<token.size();i++){
			if (token[i]=="FROM") break;
			if (token[i]==",")continue;
			output.push_back(token[i]);
			m[output.back()]=output.size()-1;
		}
		//from
		for (i++;i<token.size();i++){
			if (token[i]=="WHERE"||token[i]==";")break;
			if (token[i]==",")continue;
			table.push_back(token[i]);
		}
		//where?
		if (i<token.size()&&token[i]=="WHERE"){
			for (i++;i<token.size();i+=4){
				cond.push_back(condition(token[i],token[i+1][0],token[i+2]));
			}
		}
		row.clear();
		row.resize(output.size(),"");
		done(table,m,0,row,cond);
	}else if (token[0]=="INSERT"){
//		for (int i=0;i<token.size();i++)printf("%s ",token[i].c_str());
//		printf("\n");
		vector<vector<string> > data;
		int len=table2name[token[2]].size();
		for (int i=4;i<token.size();i+=len*2+2){
			vector<string> tmp;
			for (int j=0;j<len;j++){
				tmp.push_back(token[i+j*2+1]);
			}
			data.push_back(tmp);
		}
		FILE *data_file=fopen(((string) "data/" + token[2]).c_str(), "a");
		for (int i=0;i<data.size();i++){
			fprintf(data_file,"%s",data[i][0].c_str());
			for (int j=1;j<len;j++)fprintf(data_file,",%s",data[i][j].c_str());
			fprintf(data_file,"\n");
		}
		fclose(data_file);
	}
}

int next(char *row)
{
	if (result.size() == 0)
		return (0);
	strcpy(row, result.back().c_str());
	result.pop_back();

	/*
	 * This is for debug only. You should avoid unnecessary output
	 * in your submission, which will hurt the performance.
	 */
	printf("%s\n", row);

	return (1);
}

void close()
{
	// I have nothing to do.
}




