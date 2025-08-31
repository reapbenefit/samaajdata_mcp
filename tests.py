"""
Comprehensive tests for all plotting tools.
"""

import unittest
from unittest.mock import Mock, patch, AsyncMock
import sys
import os

# Add the project root to sys.path to import server modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import (
    create_pie_chart,
    create_bar_chart,
    create_line_plot,
    create_histogram,
    create_box_plot,
    create_scatter_plot,
    create_donut_chart,
    create_heatmap,
    create_area_chart,
)
from mcp.server.fastmcp import Context


class TestPlottingTools(unittest.IsolatedAsyncioTestCase):
    """Test suite for all plotting functions."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock(spec=Context)
        self.mock_ctx.debug = AsyncMock()

        self.mock_upload_result = {
            "success": True,
            "bucket": "test-bucket",
            "key": "test-folder/test-image.png",
            "public_url": "https://test-cloudfront.net/test-folder/test-image.png",
            "message": "Image uploaded successfully to S3",
        }

    # ========================================
    # PIE CHART TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_pie_chart_basic_success(self, mock_upload, mock_insert):
        """Test basic pie chart creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_pie_chart(
            ctx=self.mock_ctx,
            data=["Apple", "Apple", "Orange", "Banana"],
            title="Test Chart",
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])
        self.assertEqual(result["title"], "Test Chart")

    @patch("server.insert_query")
    async def test_pie_chart_empty_data(self, mock_insert):
        """Test pie chart with empty data."""
        mock_insert.return_value = None

        result = await create_pie_chart(ctx=self.mock_ctx, data=[])

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    async def test_pie_chart_none_data(self, mock_insert):
        """Test pie chart with None data."""
        mock_insert.return_value = None

        result = await create_pie_chart(ctx=self.mock_ctx, data=None)

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    async def test_pie_chart_invalid_data_type(self, mock_insert):
        """Test pie chart with invalid data type."""
        mock_insert.return_value = None

        result = await create_pie_chart(ctx=self.mock_ctx, data="invalid")

        self.assertIn("error", result)
        self.assertIn("Data must be a list", result["error"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_pie_chart_single_item(self, mock_upload, mock_insert):
        """Test pie chart with single item."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_pie_chart(ctx=self.mock_ctx, data=["Single"])

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_pie_chart_no_percentages(self, mock_upload, mock_insert):
        """Test pie chart with percentages disabled."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_pie_chart(
            ctx=self.mock_ctx, data=["A", "B", "A"], show_percentages=False
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3", side_effect=Exception("Upload failed"))
    async def test_pie_chart_upload_failure(self, mock_upload, mock_insert):
        """Test pie chart with S3 upload failure."""
        mock_insert.return_value = None

        result = await create_pie_chart(ctx=self.mock_ctx, data=["A", "B"])

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    # ========================================
    # BAR CHART TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_bar_chart_basic_success(self, mock_upload, mock_insert):
        """Test basic bar chart creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_bar_chart(
            ctx=self.mock_ctx,
            data={"categories": ["A", "B", "C"], "values": [10, 20, 15]},
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_bar_chart_empty_data(self, mock_insert):
        """Test bar chart with empty data."""
        mock_insert.return_value = None

        result = await create_bar_chart(ctx=self.mock_ctx, data={})

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    async def test_bar_chart_missing_categories(self, mock_insert):
        """Test bar chart with missing categories."""
        mock_insert.return_value = None

        result = await create_bar_chart(
            ctx=self.mock_ctx, data={"values": [10, 20, 15]}
        )

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_bar_chart_horizontal_style(self, mock_upload, mock_insert):
        """Test bar chart with horizontal style."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_bar_chart(
            ctx=self.mock_ctx,
            data={"categories": ["A", "B"], "values": [10, 20]},
            chart_style="horizontal",
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_bar_chart_stacked_style(self, mock_upload, mock_insert):
        """Test bar chart with stacked style."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_bar_chart(
            ctx=self.mock_ctx,
            data={"categories": ["A", "B"], "Series1": [10, 20], "Series2": [5, 15]},
            chart_style="stacked",
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_bar_chart_raw_data_format(self, mock_upload, mock_insert):
        """Test bar chart with raw data format."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_bar_chart(
            ctx=self.mock_ctx, data={"raw_data": ["A", "A", "B", "B", "B"]}
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    # ========================================
    # LINE PLOT TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_line_plot_basic_success(self, mock_upload, mock_insert):
        """Test basic line plot creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_line_plot(
            ctx=self.mock_ctx, data={"x": [1, 2, 3, 4], "y": [10, 20, 15, 25]}
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_line_plot_missing_x_axis(self, mock_insert):
        """Test line plot with missing x-axis data."""
        mock_insert.return_value = None

        result = await create_line_plot(ctx=self.mock_ctx, data={"y": [10, 20, 15]})

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    async def test_line_plot_mismatched_lengths(self, mock_insert):
        """Test line plot with mismatched data lengths."""
        mock_insert.return_value = None

        result = await create_line_plot(
            ctx=self.mock_ctx, data={"x": [1, 2, 3], "y": [10, 20]}
        )

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_line_plot_multi_series(self, mock_upload, mock_insert):
        """Test line plot with multiple series."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_line_plot(
            ctx=self.mock_ctx,
            data={"x": [1, 2, 3], "Series1": [10, 15, 20], "Series2": [5, 10, 15]},
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    # ========================================
    # HISTOGRAM TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_histogram_basic_success(self, mock_upload, mock_insert):
        """Test basic histogram creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_histogram(
            ctx=self.mock_ctx, data=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_histogram_empty_data(self, mock_insert):
        """Test histogram with empty data."""
        mock_insert.return_value = None

        result = await create_histogram(ctx=self.mock_ctx, data=[])

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    async def test_histogram_invalid_data_type(self, mock_insert):
        """Test histogram with invalid data type."""
        mock_insert.return_value = None

        result = await create_histogram(ctx=self.mock_ctx, data="invalid")

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    async def test_histogram_non_numeric_data(self, mock_insert):
        """Test histogram with non-numeric data."""
        mock_insert.return_value = None

        result = await create_histogram(ctx=self.mock_ctx, data=["a", "b", "c"])

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    # ========================================
    # BOX PLOT TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_box_plot_basic_success(self, mock_upload, mock_insert):
        """Test basic box plot creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_box_plot(
            ctx=self.mock_ctx,
            data={"Group A": [1, 2, 3, 4, 5], "Group B": [2, 3, 4, 5, 6]},
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_box_plot_empty_data(self, mock_insert):
        """Test box plot with empty data."""
        mock_insert.return_value = None

        result = await create_box_plot(ctx=self.mock_ctx, data={})

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_box_plot_horizontal_orientation(self, mock_upload, mock_insert):
        """Test box plot with horizontal orientation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_box_plot(
            ctx=self.mock_ctx,
            data={"Group A": [1, 2, 3, 4, 5]},
            orientation="horizontal",
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    # ========================================
    # SCATTER PLOT TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_scatter_plot_basic_success(self, mock_upload, mock_insert):
        """Test basic scatter plot creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_scatter_plot(
            ctx=self.mock_ctx, data={"x": [1, 2, 3, 4], "y": [2, 4, 1, 5]}
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_scatter_plot_missing_y_data(self, mock_insert):
        """Test scatter plot with missing y data."""
        mock_insert.return_value = None

        result = await create_scatter_plot(ctx=self.mock_ctx, data={"x": [1, 2, 3, 4]})

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    async def test_scatter_plot_mismatched_lengths(self, mock_insert):
        """Test scatter plot with mismatched x and y lengths."""
        mock_insert.return_value = None

        result = await create_scatter_plot(
            ctx=self.mock_ctx, data={"x": [1, 2, 3], "y": [2, 4]}
        )

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_scatter_plot_with_groups(self, mock_upload, mock_insert):
        """Test scatter plot with grouping."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_scatter_plot(
            ctx=self.mock_ctx,
            data={"x": [1, 2, 3, 4], "y": [2, 4, 1, 5], "group": ["A", "A", "B", "B"]},
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    # ========================================
    # DONUT CHART TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_donut_chart_basic_success(self, mock_upload, mock_insert):
        """Test basic donut chart creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_donut_chart(
            ctx=self.mock_ctx, data=["Apple", "Orange", "Apple", "Banana"]
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_donut_chart_empty_data(self, mock_insert):
        """Test donut chart with empty data."""
        mock_insert.return_value = None

        result = await create_donut_chart(ctx=self.mock_ctx, data=[])

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_donut_chart_with_center_text(self, mock_upload, mock_insert):
        """Test donut chart with center text."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_donut_chart(
            ctx=self.mock_ctx, data=["A", "B", "A"], center_text="Total: 3"
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    # ========================================
    # HEATMAP TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_heatmap_basic_success(self, mock_upload, mock_insert):
        """Test basic heatmap creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_heatmap(
            ctx=self.mock_ctx,
            data={
                "matrix": [[1, 2, 3], [4, 5, 6]],
                "x_labels": ["A", "B", "C"],
                "y_labels": ["X", "Y"],
            },
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_heatmap_empty_data(self, mock_insert):
        """Test heatmap with empty data."""
        mock_insert.return_value = None

        result = await create_heatmap(ctx=self.mock_ctx, data={})

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    async def test_heatmap_invalid_format(self, mock_insert):
        """Test heatmap with invalid data format."""
        mock_insert.return_value = None

        result = await create_heatmap(ctx=self.mock_ctx, data={"invalid": "format"})

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_heatmap_pivot_format(self, mock_upload, mock_insert):
        """Test heatmap with pivot format data."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_heatmap(
            ctx=self.mock_ctx,
            data={"x": ["A", "A", "B"], "y": ["X", "Y", "X"], "values": [1, 2, 3]},
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    # ========================================
    # AREA CHART TESTS
    # ========================================

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_area_chart_basic_success(self, mock_upload, mock_insert):
        """Test basic area chart creation."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_area_chart(
            ctx=self.mock_ctx, data={"x": [1, 2, 3, 4], "y": [10, 15, 12, 18]}
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_area_chart_missing_x_data(self, mock_insert):
        """Test area chart with missing x-axis data."""
        mock_insert.return_value = None

        result = await create_area_chart(ctx=self.mock_ctx, data={"y": [10, 15, 12]})

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_area_chart_stacked_style(self, mock_upload, mock_insert):
        """Test area chart with stacked style."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_area_chart(
            ctx=self.mock_ctx,
            data={"x": [1, 2, 3], "Series1": [10, 15, 12], "Series2": [5, 8, 6]},
            chart_style="stacked",
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    @patch("server.upload_image_to_s3")
    async def test_area_chart_overlapping_style(self, mock_upload, mock_insert):
        """Test area chart with overlapping style."""
        mock_insert.return_value = None
        mock_upload.return_value = self.mock_upload_result

        result = await create_area_chart(
            ctx=self.mock_ctx,
            data={"x": [1, 2, 3], "Series1": [10, 15, 12], "Series2": [5, 8, 6]},
            chart_style="overlapping",
        )

        self.assertEqual(result["public_url"], self.mock_upload_result["public_url"])

    @patch("server.insert_query")
    async def test_area_chart_mismatched_lengths(self, mock_insert):
        """Test area chart with mismatched data lengths."""
        mock_insert.return_value = None

        result = await create_area_chart(
            ctx=self.mock_ctx, data={"x": [1, 2, 3], "y": [10, 15]}
        )

        self.assertIn("error", result)
        self.assertIsNone(result["public_url"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
