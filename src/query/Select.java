package query;

import heap.HeapFile;
import parser.AST_Select;
import relop.*;

import java.util.ArrayList;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan
{
	private boolean isExplain = false;
	private ArrayList<Iterator> scans = new ArrayList<>();
	private Projection projection;

	/**
	 * Optimizes the plan, given the parsed query.
	 *
	 * @throws QueryException if validation fails
	 */
	public Select(AST_Select tree) throws QueryException
	{
		isExplain = tree.isExplain;
		ArrayList<FileScan> fileScans = new ArrayList<>();
		ArrayList<Schema> schemas = new ArrayList<>();
		ArrayList<Integer> columnNums = new ArrayList<>();

		//Check and make sure tables are valid
		for (String table : tree.getTables())
		{
			try
			{
				Schema schema = QueryCheck.tableExists(table);
				schemas.add(schema);
				FileScan fileScan = new FileScan(schema, new HeapFile(table));
				fileScans.add(fileScan);
				scans.add(fileScan);
			}
			catch (QueryException exception)
			{
				closeAllScans();
				throw exception;
			}
		}



		//Make sure no column doesn't exist
		for (String columnName : tree.getColumns())
		{
			boolean validColumn = false;
			for (Schema schema : schemas)
			{
				try
				{
					QueryCheck.columnExists(schema, columnName);
					validColumn = true;
					break;
				}
				catch (QueryException ignored) {

				}
			}

			if (!validColumn)
			{
				closeAllScans();
				throw new QueryException("column '" + columnName + "' doesn't exist");
			}
		}

		//Everything else

		//Only One Table
		if (schemas.size() == 1)
		{
			try
			{
				QueryCheck.predicates(schemas.get(0), tree.getPredicates());
			}
			catch (QueryException exception)
			{
				closeAllScans();
				throw exception;
			}

			//No Predicates
			if (tree.getPredicates().length == 0)
			{
				System.out.println(fileScans.size());
				Selection selection = new Selection(fileScans.get(0));
				scans.add(selection);
			}
			else    //Some predicates
			{
				try
				{
					QueryCheck.predicates(schemas.get(0), tree.getPredicates());
				}
				catch (QueryException exception)
				{
					closeAllScans();
					throw exception;
				}
				for (Predicate[] predicates : tree.getPredicates())
				{
					Selection selection = new Selection(scans.get(scans.size() - 1), predicates);
					scans.add(selection);
				}
			}

			//We have a '*'
			if (tree.getColumns().length == 0)
			{
				columnNums = new ArrayList<>();
				for (int i = 0; i < schemas.get(0).getCount(); i++)
				{
					columnNums.add(i);
				}
			}
			else
			{
				for (String columnName : tree.getColumns())
				{
					columnNums.add(QueryCheck.columnExists(schemas.get(0), columnName));
				}
			}
		}
		else    //More than one table.  We need to join
		{
			//First 2 manually
			Schema joinedScheme = Schema.join(schemas.get(0), schemas.get(1));
			SimpleJoin join = new SimpleJoin(fileScans.get(0), fileScans.get(1));
			join.setSchema(joinedScheme);
			scans.add(join);

			//Rest of the tables
			for (int i = 2; i < schemas.size() - 1; i++)
			{
				joinedScheme = Schema.join(joinedScheme, schemas.get(i));
				SimpleJoin newjoin = new SimpleJoin(fileScans.get(i), fileScans.get(i + 1));
				newjoin.setSchema(joinedScheme);
				scans.add(newjoin);
			}

			//We have some predicates
			if (tree.getPredicates().length != 0)
			{
				try
				{
					QueryCheck.predicates(joinedScheme, tree.getPredicates());
				}
				catch (QueryException exception)
				{
					closeAllScans();
					throw exception;
				}

				for (Predicate[] predicates : tree.getPredicates())
				{
					Selection selection = new Selection(scans.get(scans.size() - 1), predicates);
					scans.add(selection);
				}
			}
			else //We have no predicates.  Full Cross Product
			{
				Selection selection = new Selection(scans.get(scans.size() - 1));
				scans.add(selection);
			}

			//We have a '*'
			columnNums = new ArrayList<>();
			if (tree.getColumns().length == 0)
			{
				for (int i = 0; i < joinedScheme.getCount(); i++)
				{
					columnNums.add(i);
				}
			}
			else    //Specific column names
			{
				for (String columnName : tree.getColumns())
				{
					columnNums.add(QueryCheck.columnExists(joinedScheme, columnName));
				}
			}
		}

		//Make the projection
		projection = new Projection(scans.get(scans.size() - 1), columnNums.toArray(new Integer[columnNums.size()]));
		scans.add(projection);

	} // public Select(AST_Select tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute()
	{

		if (isExplain) //Explain
		{
			projection.explain(0);
		}
		else
		{
			if (projection == null)
			{
				closeAllScans();
				return;
			}
			int count = projection.execute();
			System.out.println(count + (count != 1 ? " rows" : " row") + " affected.");
		}
		closeAllScans();
	} // public void execute()


	private void closeAllScans()
	{
		if (projection != null)
		{
			projection.close();
			return;
		}
		for (Iterator scan : scans)
		{
			scan.close();
		}
	}
} // class Select implements Plan
