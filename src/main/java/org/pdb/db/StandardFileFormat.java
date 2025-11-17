package org.pdb.db;

/**
 * Implements a file format that expects header lines starting with 'H|', trailer lines starting with 'T|' and
 * the rest of the files containing one line records.
 * 
 * Override this class to deviate from the regular pipe-delimited text file format.
 */
public class StandardFileFormat implements FileFormat {

}
