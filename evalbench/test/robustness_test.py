from evaluator.evaluator import Evaluator
from queue import Queue
from unittest.mock import MagicMock, patch
from unittest.mock import MagicMock, patch, call
from util.rate_limit import rate_limit, ResourceExhaustedError
from work.sqlexecwork import SQLExecWork
import concurrent.futures
import logging
import threading
import unittest


class TestEvaluatorRobustness(unittest.TestCase):

    def setUp(self):
        self.config = {
            "runners": {"prompt_runners": 1, "sqlgen_runners": 1, "sqlexec_runners": 1, "scoring_runners": 1},
            "max_executions_per_minute": 60
        }
        
        # We need to mock the runners because they start threads
        with patch('mp.mprunner.MPRunner'):
            self.evaluator = Evaluator(self.config)
            self.evaluator.promptrunner = MagicMock()
            self.evaluator.genrunner = MagicMock()
            self.evaluator.sqlrunner = MagicMock()
            self.evaluator.scoringrunner = MagicMock()

    @patch('concurrent.futures.as_completed')
    def test_evaluator_handles_as_completed_timeout(self, mock_as_completed):
        import concurrent.futures
        # Simulate concurrent.futures.as_completed raising TimeoutError
        mock_as_completed.side_effect = concurrent.futures.TimeoutError()
        
        progress_reporting = {"prompt_i": MagicMock(), "total": 10}
        
        with self.assertLogs('root', level='ERROR') as cm:
            self.evaluator.evaluate(
                dataset=[MagicMock()], 
                db_queue=MagicMock(),
                prompt_generator=MagicMock(),
                model_generator=MagicMock(),
                job_id="test", 
                run_time=MagicMock(), 
                progress_reporting=progress_reporting,
                global_models={}
            )
        
        self.assertTrue(any("phase timed out" in output for output in cm.output))

    @patch('concurrent.futures.as_completed')
    def test_evaluator_handles_future_exception(self, mock_as_completed):
        # Simulate a future that raises an exception when result() is called
        mock_future = MagicMock()
        mock_future.result.side_effect = Exception("Worker crashed")
        mock_as_completed.return_value = [mock_future]
        
        progress_reporting = MagicMock()
        
        with self.assertLogs('root', level='ERROR') as cm:
             self.evaluator.evaluate(
                dataset=[MagicMock()], 
                db_queue=MagicMock(),
                prompt_generator=MagicMock(),
                model_generator=MagicMock(),
                job_id="test", 
                run_time=MagicMock(), 
                progress_reporting=progress_reporting,
                global_models={}
            )
             
        self.assertTrue(any("future error: Worker crashed" in output for output in cm.output))


class TestStability(unittest.TestCase):

    def test_rate_limit_guaranteed_release(self):
        semaphore = MagicMock()
        execution_method = MagicMock(side_effect=Exception("Execution failed"))
        
        # When execution_method raises an exception, the semaphore MUST still be released.
        with self.assertRaises(Exception) as cm:
            rate_limit(
                query=("SELECT 1",),
                execution_method=execution_method,
                execs_per_minute=60,
                semaphore=semaphore,
                max_attempts=1
            )
        
        self.assertEqual(str(cm.exception), "Execution failed")
        semaphore.release.assert_called_once()

    def test_sqlexecwork_guaranteed_queue_return(self):
        db = MagicMock()
        db_queue = Queue()
        eval_result = {"sql_generator_error": "Some error", "query_type": "dql"}
        
        work = SQLExecWork(db, {}, eval_result, db_queue)
        
        # Mock _run_inner to raise an exception
        work._run_inner = MagicMock(side_effect=RuntimeError("Inner crash"))
        
        with self.assertRaises(RuntimeError):
            work.run()
            
        # The DB object MUST have been returned to the queue despite the crash
        self.assertEqual(db_queue.get_nowait(), db)

    def test_sqlexecwork_handles_empty_query_safely(self):
        db = MagicMock()
        db_queue = Queue()
        eval_result = {
            "sql_generator_error": None, 
            "generated_sql": "   ", 
            "query_type": "dql",
            "eval_query": [],
            "golden_sql": "",
            "preprocess_sql": []
        }
        config = {
            "prompt_generator": "NOOPGenerator",
            "dialect": "sqlite"
        }
        
        work = SQLExecWork(db, config, eval_result, db_queue)
        
        # Should not raise "list index out of range"
        result = work.run()
        
        self.assertIsNone(result.get("generated_result"))
        self.assertEqual(result.get("generated_error"), "list index out of range (empty query)")

    @patch('concurrent.futures.as_completed')
    def test_oneshotorchestrator_timeout(self, mock_as_completed):
        from evaluator.oneshotorchestrator import OneShotOrchestrator
        config = {"max_executions_per_minute": 60}
        
        orchestrator = OneShotOrchestrator(config, [], {})
        
        # Simulate 24 hour timeout
        mock_as_completed.side_effect = concurrent.futures.TimeoutError()
        
        with self.assertLogs('root', level='ERROR') as cm:
            # We need to mock breakdown_datasets or provided enough data
            with patch('dataset.evalinput.breakdown_datasets', return_value=({}, 0, 0)):
                orchestrator.evaluate([])
        
        self.assertTrue(any("thread timed out" in out for out in cm.output))



if __name__ == '__main__':
    unittest.main()
