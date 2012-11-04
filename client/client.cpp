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


/*void done(const vector<string>& table, const map<string, int>& m,
	int depth, vector<string>& row)
{
	FILE *fin;
	char buf[65536];
	vector<string> column_name, token;
	string str;
	int i;

	if (depth == table.size()) {
		str = row[0];
		for (i = 1; i < row.size(); i++)
			str += "," + row[i];
		result.push_back(str);
		return;	
	}

	assert(table2name.find(table[depth]) != table2name.end());
	column_name = table2name[table[depth]];

	fin = fopen(((string) "data/" + table[depth]).c_str(), "r");
	assert(fin != NULL);

	while (fgets(buf, 65536, fin) != NULL) {
		int len = strlen(buf);
		if (len > 0 && buf[len - 1] == '\n') {
			buf[len - 1] = '\0';
			len--;
		}
		if (len == 0)
			continue;

		split_csv(buf, token);
		assert(token.size() == column_name.size());

		for (i = 0; i < column_name.size(); i++)
			if (m.find(column_name[i]) != m.end())
				row[m.find(column_name[i]) -> second] = token[i];

		done(table, m, depth + 1, row);
	}

	fclose(fin);
}*/

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
	vector<string> token, output, table, row;
	vector<condition> cond;
	map<string, int> m;
	
	int i;

	result.clear();

/*	if (strstr(sql.c_str(), "INSERT") != NULL ||
		strstr(sql.c_str(), "WHERE") != NULL) {
		fprintf(stderr, "Sorry, I give up.\n");
		exit(1);
	}*/
	
	
	output.clear();
	table.clear();
	tokenize(sql.c_str(), token);
	printf("%s\n",sql.c_str());
	if (token[0]=="SELECT"){
		int i;
		for (i=1;i<token.size();i++){
			if (token[i]=="FROM") break;
			if (token[i]==",")continue;
			output.push_back(token[i]);
			m[output.back()]=output.size()-1;
		}
//		for (map<string,int>::iterator it=m.begin();it!=m.end();it++){
//			printf("(%s %d)", it->first.c_str(),it->second);
//		}
//		printf("\n");

//		for (int j=0;j<output.size();j++){
//			printf("%s ",output[j].c_str());
//		}
//		printf("\n");
		//from
		for (i++;i<token.size();i++){
			if (token[i]=="WHERE"||token[i]==";")break;
			if (token[i]==",")continue;
			table.push_back(token[i]);
		}
//		for (int j=0;j<table.size();j++){
//			printf("%s ",table[j].c_str());
//		}
//		printf("\n");
		//where?
		if (i<token.size()&&token[i]=="WHERE"){
			for (i++;i<token.size();i+=4){
//				if (token[i]==";"||token[i]=="AND")continue;
//				printf("%s %s %s\n",token[i].c_str(),token[i+1].c_str(),token[i+2].c_str());
				cond.push_back(condition(token[i],token[i+1][0],token[i+2]));
//				printf("%s %c",cond.back().left.c_str(),cond[i].op);
//				if (cond[i].type==0)printf("%d\n",cond.back().constant);
//				else printf("%s\n",cond.back().right.c_str());

			}
		}
		row.clear();
		row.resize(output.size(),"");
		done(table,m,0,row,cond);
	}else if (token[0]=="INSERT"){
	}
	
/*	for (i = 0; i < token.size(); i++) {
		if (token[i] == "SELECT" || token[i] == ",")
			continue;
		if (token[i] == "FROM")
			break;
		output.push_back(token[i]);
	}
	for (i++; i < token.size(); i++) {
		if (token[i] == "," || token[i] == ";")
			continue;
		table.push_back(token[i]);
	}

	m.clear();
	for (i = 0; i < output.size(); i++)
		m[output[i]] = i;

	row.clear();
	row.resize(output.size(), "");

	done(table, m, 0, row);*/
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



