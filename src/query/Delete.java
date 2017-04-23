package query;

import global.Minibase;
import heap.HeapFile;
import parser.AST_Delete;
import relop.FileScan;
import relop.Predicate;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan
{
	private String fileName;
	private Schema schema;
	private Predicate[][] predicates;

	/**
	 * Optimizes the plan, given the parsed query.
	 *
	 * @throws QueryException if table doesn't exist or predicates are invalid
	 */
	public Delete(AST_Delete tree) throws QueryException
	{
		fileName = tree.getFileName();
		QueryCheck.tableExists(fileName);
		schema = Minibase.SystemCatalog.getSchema(fileName);

		if (tree.getPredicates().length > 0) { QueryCheck.predicates(schema, tree.getPredicates()); }

		predicates = tree.getPredicates();
	} // public Delete(AST_Delete tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute()
	{
		HeapFile file = new HeapFile(fileName);
		int count = 0;
		FileScan scan = new FileScan(schema, file);
		while (scan.hasNext())
		{
			Tuple row = scan.getNext();
			if (predicates.length != 0)
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

				if (satisfiesAllPredicates)
				{
					file.deleteRecord(scan.getLastRID());
					count++;
				}
			}
			else
			{
				file.deleteRecord(scan.getLastRID());
				count++;
			}
		}
		scan.close();

		Minibase.SystemCatalog.updateRecordCountInTableBy(fileName, -count);

		System.out.println(count + (count != 1 ? " rows" : " row") + " affected.");
	} // public void execute()

} // class Delete implements Plan
