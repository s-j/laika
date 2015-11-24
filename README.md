laika
=====

A distributed query processing framework written during my PhD study. Implements a term lexicon, a document dictionary and an inverted index with dynamic pruning and skipping. It supports query evaluation over a term-wise or document-wise partitioned index, with emphasis on pipelined query processing with a term-wise distributed index. The code is shared rather for inspiration and implementations insight than for a further reuse – the implementation was done in a rush, with no unit tests, and there are just too many things I would have done differently today.

Different variants of this code have been used in the following papers:
* Improving Dynamic Index Pruning via Linear Programming. Simon Jonassen. LSDS-IR@CIKM 2015: 21-25
* Improving the performance of pipelined query processing with skipping - and its comparison to document-wise partitioning. Simon Jonassen, Svein Erik Bratsberg. World Wide Web 17(5): 949-967 (2014)
* A term-based inverted index partitioning model for efficient distributed query processing. Berkant Barla Cambazoglu, Enver Kayaaslan, Simon Jonassen, Cevdet Aykanat. TWEB 7(3): 15 (2013)
* Intra-query Concurrent Pipelined Processing for Distributed Full-Text Retrieval. Simon Jonassen, Svein Erik Bratsberg, ECIR 2012: 413-425
* Improving the Performance of Pipelined Query Processing with Skipping. Simon Jonassen, Svein Erik Bratsberg, WISE 2012: 1-15
* Efficient Compressed Inverted Index Skipping for Disjunctive Text-Queries. Simon Jonassen, Svein Erik Bratsberg, ECIR 2011: 530-542
