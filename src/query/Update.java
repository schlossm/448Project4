package query;

import heap.HeapFile;
import parser.AST_Update;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for updating tuples.
 */
class Update implements Plan
{
	private String fileName;
	private Schema schema;
	private String[] columns;
	private Object[] values;
	private Predicate[][] predicates;

	/**
	 * Optimizes the plan, given the parsed query.
	 *
	 * @throws QueryException if invalid column names, values, or pedicates
	 */
	public Update(AST_Update tree) throws QueryException
	{
		fileName = tree.getFileName();
		schema = QueryCheck.tableExists(fileName);

		QueryCheck.updateValues(schema, QueryCheck.updateFields(schema, tree.getColumns()), tree.getValues());

		columns = tree.getColumns();
		values = tree.getValues();

		QueryCheck.predicates(schema, tree.getPredicates());
		predicates = tree.getPredicates();
	} // public Update(AST_Update tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute()
	{
		HeapFile file = new HeapFile(fileName);
		FileScan scan = new FileScan(schema, file);
		int count = 0;

		while (scan.hasNext())
		{
			Tuple row = scan.getNext();
			if (predicates.length != 0) //Update rows that satisfy all predicates
			{
				boolean satisfiesAllPredicates = true;
				for (Predicate[] predicates : this.predicates)
				{
					boolean satisfiesAPredicateInGroup = false;
					for (Predicate predicate : predicates)
					{
						if (predicate.evaluate(row))
						{
							satisfiesAPredicateInGroup = true;
							break;
						}
					}

					if (!satisfiesAPredicateInGroup)
					{
						satisfiesAllPredicates = false;
						break;
					}
				}

				if (satisfiesAllPredicates) //We should update this row
				{
					for (int i = 0; i < columns.length; i++)
					{
						row.setField(columns[i], values[i]);
					}
					file.updateRecord(scan.getLastRID(), row.getData());
					count++;
				}
			}
			else    //Update every row
			{
				for (int i = 0; i < columns.length; i++)
				{
					row.setField(columns[i], values[i]);
				}
				file.updateRecord(scan.getLastRID(), row.getData());
			}
		}
		scan.close();

		// print the output message
		System.out.println(count + (count != 1 ? " rows" : " row") + " affected.");

	} // public void execute()

} // class Update implements Plan
