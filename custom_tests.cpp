std::vector<int> insertedValues;

void createRelationSparse()
{
	std::vector<RecordId> ridVec;
  // destroy any old copies of relation file


	try
	{
		File::remove(relationName);
	}
	catch(FileNotFoundException e)
	{
	}

  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < 3000; i++ )
	{

	int key = (int)random()*1000000;
	insertedValues.push_back(key);
    sprintf(record1.s, "%05d string record", key);
    record1.i = key;
    record1.d = (double)key;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(InsufficientSpaceException e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}

int count(int lowVal, Operator lowerBound, int highVal,Operator upperBound){
	int result = 0;
	for(int i = 0; i < 3000; i++){
		int curr = insertedValues.at(i);
		if(lowerBound == GT && upperBound == LT){
			if(curr < highVal && curr> lowVal) result++;
		}
		else if(lowerBound == GTE && upperBound == LT){
			if(curr < highVal && curr>= lowVal) result++;
		}
		else if(lowerBound == GTE&& upperBound == LTE){
			if(curr <= highVal && curr> lowVal) result++;
		}
		else if(lowerBound == GTE && upperBound == LTE){
			if(curr <= highVal && curr>= lowVal) result++;
		}
	}
	return result;
}
