package query;

import parser.AST_Describe;
import relop.Schema;

/**
 * Execution plan for describing tables.
 */
class Describe implements Plan
{
	private Schema schema;
	/**
	 * Optimizes the plan, given the parsed query.
	 *
	 * @throws QueryException if table doesn't exist
	 */
	public Describe(AST_Describe tree) throws QueryException
	{
		schema = QueryCheck.tableExists(tree.getFileName());
	} // public Describe(AST_Describe tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute()
	{
		schema.print();
	} // public void execute()

} // class Describe implements Plan
