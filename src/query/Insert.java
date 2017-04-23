package query;

import global.Minibase;
import heap.HeapFile;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan
{
	private String fileName;
	private Schema schema;
	private Object[] values;
	/**
	 * Optimizes the plan, given the parsed query.
	 *
	 * @throws QueryException if table doesn't exists or values are invalid
	 */
	public Insert(AST_Insert tree) throws QueryException
	{
		fileName = tree.getFileName();

		schema = Minibase.SystemCatalog.getSchema(fileName);

		QueryCheck.insertValues(schema, tree.getValues());
		values = tree.getValues();
	} // public Insert(AST_Insert tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute()
	{
		HeapFile table = new HeapFile(fileName);
		table.insertRecord(new Tuple(schema, values).getData());
		Minibase.SystemCatalog.updateRecordCountInTableBy(fileName, 1);

		System.out.println("1 row affected.");
	} // public void execute()

} // class Insert implements Plan
